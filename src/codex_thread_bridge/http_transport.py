"""Streamable-HTTP MCP transport on a private unix socket, guarded by a Bearer token.

Intended for running the bridge next to the App Server and reaching it through an ssh socket
forward. The socket lives in a user-owned 700 directory and is created 600; every request must
carry `Authorization: Bearer <token>` (constant-time comparison, 401 otherwise). The default MCP
transport stays stdio; nothing here is imported unless --mcp-transport streamable-http is used.
"""

from __future__ import annotations

import asyncio
import hmac
import os
import signal
import socket
import stat
from pathlib import Path

MAX_TOKEN_BYTES = 4096
MIN_TOKEN_CHARS = 16


class TransportConfigError(Exception):
    """The socket or token file does not meet the safety requirements."""


def read_token_file(path: Path) -> bytes:
    """Read a Bearer token; the file must be a user-owned regular file with mode 600."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_NONBLOCK", 0))
    except OSError as exc:
        raise TransportConfigError(f"cannot open token file {path}: {exc.strerror}") from exc
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise TransportConfigError(f"token file {path} must be a regular file")
        if info.st_uid != os.geteuid():
            raise TransportConfigError(f"token file {path} must be owned by the current user")
        if stat.S_IMODE(info.st_mode) != 0o600:
            raise TransportConfigError(f"token file {path} must have mode 600")
        if info.st_size > MAX_TOKEN_BYTES:
            raise TransportConfigError(f"token file {path} is too large")
        data = os.read(fd, MAX_TOKEN_BYTES + 1)
    finally:
        os.close(fd)
    token = data.strip()
    if len(token) < MIN_TOKEN_CHARS:
        raise TransportConfigError(f"token in {path} must be at least {MIN_TOKEN_CHARS} characters")
    if any(b <= 0x20 or b >= 0x7F for b in token):
        raise TransportConfigError(f"token in {path} must be printable ASCII without spaces")
    return token


def _is_live(path: str) -> bool:
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    probe.settimeout(2)
    try:
        probe.connect(path)
    except (ConnectionRefusedError, FileNotFoundError):
        return False
    except OSError:
        return True  # unknown error: do not delete someone else's socket
    finally:
        probe.close()
    return True


def bind_private_socket(path: Path) -> socket.socket:
    """Bind a listening unix socket with mode 600 in an existing user-owned 700 directory.

    Refuses a symlink or a non-socket at `path` and a socket another process is serving; removes
    only a stale socket (nobody accepts on it).
    """
    if not path.is_absolute():
        raise TransportConfigError("--mcp-socket must be an absolute path")
    parent = path.parent
    try:
        pinfo = parent.lstat()
    except OSError as exc:
        raise TransportConfigError(f"socket directory {parent} does not exist") from exc
    if not stat.S_ISDIR(pinfo.st_mode) or pinfo.st_uid != os.geteuid():
        raise TransportConfigError(f"{parent} must be a user-owned directory, not a symlink")
    if stat.S_IMODE(pinfo.st_mode) != 0o700:
        raise TransportConfigError(f"socket directory {parent} must have mode 700")
    if len(os.fsencode(str(path))) > 100:
        raise TransportConfigError(f"socket path is too long for a unix socket: {path}")
    try:
        info = path.lstat()
    except FileNotFoundError:
        info = None
    if info is not None:
        if not stat.S_ISSOCK(info.st_mode):
            raise TransportConfigError(f"{path} exists and is not a socket; refusing to replace it")
        if info.st_uid != os.geteuid():
            raise TransportConfigError(f"{path} is not owned by the current user")
        if _is_live(str(path)):
            raise TransportConfigError(f"{path} is already served by another process")
        path.unlink()
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    previous = os.umask(0o177)
    try:
        sock.bind(str(path))
    except OSError:
        sock.close()
        raise
    finally:
        os.umask(previous)
    os.chmod(path, 0o600)
    sock.listen(128)
    sock.setblocking(False)
    return sock


class BearerAuth:
    """Pure ASGI middleware: 401 unless `Authorization: Bearer <token>` matches."""

    def __init__(self, app, token: bytes):
        self.app = app
        self.token = token

    async def __call__(self, scope, receive, send):
        if scope["type"] == "lifespan":
            await self.app(scope, receive, send)
            return
        if scope["type"] != "http":
            if scope["type"] == "websocket":
                await send({"type": "websocket.close", "code": 1008})
            return
        supplied = b""
        for name, value in scope.get("headers", ()):
            if name == b"authorization":
                scheme, _, rest = value.partition(b" ")
                if scheme.lower() == b"bearer":
                    supplied = rest.strip()
                break
        if not hmac.compare_digest(supplied, self.token):
            body = b'{"error":"unauthorized"}'
            await send(
                {
                    "type": "http.response.start",
                    "status": 401,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (b"content-length", str(len(body)).encode()),
                        (b"www-authenticate", b'Bearer realm="codex-thread-bridge"'),
                    ],
                }
            )
            await send({"type": "http.response.body", "body": body})
            return
        await self.app(scope, receive, send)


def build_app(mcp, token: bytes):
    """The FastMCP streamable-HTTP app (/mcp) wrapped in Bearer auth."""
    return BearerAuth(mcp.streamable_http_app(), token)


async def serve(app, sock: socket.socket, socket_path: Path) -> None:
    import uvicorn

    config = uvicorn.Config(
        app, fd=sock.fileno(), log_level="warning", lifespan="on", server_header=False
    )
    server = uvicorn.Server(config)
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        # Stop gracefully and return, instead of uvicorn's default of re-raising the signal after
        # shutdown, which would skip the cleanup below and in the caller.
        loop.add_signal_handler(signum, lambda: setattr(server, "should_exit", True))
    try:
        await server._serve()
    finally:
        # The directory is private and we hold the only live listener on this path.
        try:
            socket_path.unlink()
        except OSError:
            pass
        sock.close()


def run(mcp, socket_path: Path, token: bytes) -> None:
    sock = bind_private_socket(socket_path)
    asyncio.run(serve(build_app(mcp, token), sock, socket_path))
