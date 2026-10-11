"""Streamable-HTTP MCP transport on a private unix socket with Bearer auth."""

import asyncio
import os
import shutil
import socket as socketlib
import stat
import subprocess
import sys
import tempfile
from pathlib import Path

import httpx
import pytest
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

from codex_thread_bridge.http_transport import (
    TransportConfigError,
    bind_private_socket,
    read_token_file,
)

TOKEN = "t0ken-" + "x" * 32
INIT = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "protocolVersion": "2025-03-26",
        "capabilities": {},
        "clientInfo": {"name": "test", "version": "0"},
    },
}
MCP_HEADERS = {"accept": "application/json, text/event-stream", "content-type": "application/json"}


@pytest.fixture
def private_dir():
    path = Path(tempfile.mkdtemp(prefix="ctb-h-"))  # mode 700
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def write_token(directory: Path, text: str = TOKEN, mode: int = 0o600) -> Path:
    path = directory / "token"
    path.write_text(text + "\n")
    path.chmod(mode)
    return path


# --- token file and socket checks ---------------------------------------------------------------


def test_token_file_ok(private_dir):
    assert read_token_file(write_token(private_dir)) == TOKEN.encode()


@pytest.mark.parametrize("mode", [0o644, 0o640, 0o400, 0o700])
def test_token_file_requires_mode_600(private_dir, mode):
    with pytest.raises(TransportConfigError, match="mode 600"):
        read_token_file(write_token(private_dir, mode=mode))


def test_token_file_rejects_symlink_missing_short_and_spaces(private_dir):
    real = write_token(private_dir)
    link = private_dir / "link"
    link.symlink_to(real)
    with pytest.raises(TransportConfigError):
        read_token_file(link)
    with pytest.raises(TransportConfigError):
        read_token_file(private_dir / "missing")
    with pytest.raises(TransportConfigError, match="at least"):
        read_token_file(write_token(private_dir, "short"))
    with pytest.raises(TransportConfigError, match="printable"):
        read_token_file(write_token(private_dir, "a" * 20 + " " + "b" * 20))


def test_socket_is_created_600_in_700_dir(private_dir):
    path = private_dir / "bridge.sock"
    sock = bind_private_socket(path)
    try:
        assert stat.S_IMODE(path.lstat().st_mode) == 0o600
        assert stat.S_ISSOCK(path.lstat().st_mode)
    finally:
        sock.close()


def test_socket_dir_must_be_700(private_dir):
    private_dir.chmod(0o755)
    with pytest.raises(TransportConfigError, match="mode 700"):
        bind_private_socket(private_dir / "bridge.sock")


def test_socket_refuses_missing_dir_symlink_dir_and_relative():
    with pytest.raises(TransportConfigError):
        bind_private_socket(Path("/nonexistent-ctb/bridge.sock"))
    with pytest.raises(TransportConfigError, match="absolute"):
        bind_private_socket(Path("bridge.sock"))


def test_socket_refuses_symlink_and_regular_file(private_dir):
    target = private_dir / "target"
    target.write_text("x")
    link = private_dir / "link.sock"
    link.symlink_to(target)
    with pytest.raises(TransportConfigError, match="not a socket"):
        bind_private_socket(link)
    assert target.read_text() == "x"
    with pytest.raises(TransportConfigError, match="not a socket"):
        bind_private_socket(target)
    assert target.exists()


def test_socket_replaces_stale_but_not_live(private_dir):
    path = private_dir / "bridge.sock"
    stale = socketlib.socket(socketlib.AF_UNIX)
    stale.bind(str(path))
    stale.close()  # file stays, nobody listens
    assert path.exists()
    live = bind_private_socket(path)
    try:
        with pytest.raises(TransportConfigError, match="already served"):
            bind_private_socket(path)
        assert path.exists()
    finally:
        live.close()


# --- end to end through the real server process -------------------------------------------------


@pytest.fixture
async def http_bridge(fake_server, private_dir):
    _, app_socket = fake_server
    token_file = write_token(private_dir)
    mcp_socket = private_dir / "bridge.sock"
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "codex_thread_bridge.server",
            "--socket",
            str(app_socket),
            "--state-dir",
            str(private_dir / "state"),
            "--mcp-transport",
            "streamable-http",
            "--mcp-socket",
            str(mcp_socket),
            "--mcp-token-file",
            str(token_file),
        ],
        stderr=subprocess.PIPE,
    )
    try:
        for _ in range(100):
            if mcp_socket.exists() or process.poll() is not None:
                break
            await asyncio.sleep(0.1)
        assert process.poll() is None, process.stderr.read().decode()
        yield mcp_socket, process
    finally:
        process.terminate()
        try:
            process.wait(10)
        except subprocess.TimeoutExpired:
            process.kill()
        if process.stderr:
            process.stderr.close()


def client(mcp_socket: Path, token: str | None = TOKEN) -> httpx.AsyncClient:
    headers = {"authorization": f"Bearer {token}"} if token is not None else {}
    return httpx.AsyncClient(
        transport=httpx.AsyncHTTPTransport(uds=str(mcp_socket)),
        headers=headers,
        timeout=30,
    )


URL = "http://127.0.0.1:8000/mcp"


async def test_http_requires_bearer_token(http_bridge):
    mcp_socket, _ = http_bridge
    assert stat.S_IMODE(mcp_socket.lstat().st_mode) == 0o600
    for token in (None, "wrong-" + "y" * 32, TOKEN[:-1], TOKEN + "x"):
        async with client(mcp_socket, token) as http:
            response = await http.post(URL, json=INIT, headers=MCP_HEADERS)
            assert response.status_code == 401, token
            assert "bearer" in response.headers["www-authenticate"].lower()
            assert TOKEN not in response.text
    async with client(mcp_socket) as http:
        # a non-Bearer scheme with the right secret is rejected too
        response = await http.post(
            URL, json=INIT, headers={**MCP_HEADERS, "authorization": f"Basic {TOKEN}"}
        )
        assert response.status_code == 401
        ok = await http.post(URL, json=INIT, headers=MCP_HEADERS)
        assert ok.status_code == 200
        assert ok.json()["result"]["serverInfo"]["name"] == "codex-thread-bridge"


async def test_http_sessions_share_one_bridge(http_bridge, fake_server):
    fake, _ = fake_server
    mcp_socket, _ = http_bridge
    fake.threads = {
        "thread-a": {"id": "thread-a", "cwd": "/tmp", "status": {"type": "idle"}, "turns": []}
    }
    async with client(mcp_socket) as one, client(mcp_socket) as two:
        async with (
            streamable_http_client(URL, http_client=one) as (r1, w1, _),
            streamable_http_client(URL, http_client=two) as (r2, w2, _),
            ClientSession(r1, w1) as s1,
            ClientSession(r2, w2) as s2,
        ):
            await asyncio.gather(s1.initialize(), s2.initialize())
            tools = await s1.list_tools()
            assert "list_threads" in {tool.name for tool in tools.tools}
            first, second = await asyncio.gather(
                s1.call_tool("list_threads", {}), s2.call_tool("list_threads", {})
            )
            assert not first.isError and not second.isError
            assert first.structuredContent == second.structuredContent
            ids = [t["id"] for t in first.structuredContent["data"]]
            assert ids == ["thread-a"]
            # one session ending must not close the shared bridge for the other
            again = await s2.call_tool("get_capabilities", {})
            assert not again.isError


async def test_http_survives_many_sequential_sessions(http_bridge):
    mcp_socket, _ = http_bridge
    for _ in range(3):
        async with client(mcp_socket) as http, streamable_http_client(URL, http_client=http) as (
            read,
            write,
            _,
        ):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool("get_capabilities", {})
                assert not result.isError


async def test_http_socket_removed_on_shutdown(http_bridge):
    mcp_socket, process = http_bridge
    process.terminate()
    process.wait(10)
    assert not os.path.lexists(mcp_socket)


def run_cli(*args):
    return subprocess.run(
        [sys.executable, "-m", "codex_thread_bridge.server", *args],
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_http_flags_are_validated(private_dir):
    result = run_cli("--mcp-transport", "streamable-http")
    assert result.returncode == 2 and "requires --mcp-socket" in result.stderr
    result = run_cli("--mcp-socket", str(private_dir / "x.sock"))
    assert result.returncode == 2 and "require --mcp-transport" in result.stderr
    bad = write_token(private_dir, mode=0o644)
    result = run_cli(
        "--mcp-transport",
        "streamable-http",
        "--mcp-socket",
        str(private_dir / "x.sock"),
        "--mcp-token-file",
        str(bad),
    )
    assert result.returncode == 2 and "mode 600" in result.stderr
    assert TOKEN not in result.stderr
    assert not (private_dir / "x.sock").exists()
