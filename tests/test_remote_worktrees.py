"""Remote worktrees through command/exec: argv, validation, success and failure paths."""

import json
import sys

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from codex_thread_bridge import remote_worktrees as rw
from codex_thread_bridge.bridge import Bridge
from codex_thread_bridge.ledger import Ledger
from codex_thread_bridge.rpc import AppServer

ROOT = "/code"
WT = "/code/.worktrees"


@pytest.fixture
async def remote(fake_server, tmp_path):
    fake, socket = fake_server
    rpc = AppServer(socket, timeout=2, decline_server_requests=True, retry_reads=True)
    ledger = Ledger(tmp_path / "state" / "operations.sqlite3")
    try:
        yield fake, Bridge(rpc, ledger, remote_root=ROOT)
    finally:
        await rpc.close()
        ledger.close()


def execs(fake):
    return [p for m, p in fake.calls if m == "command/exec"]


# --- pure validation -----------------------------------------------------------------------------
@pytest.mark.parametrize("name", ["a", "charm-codex-wt-smoke", "t1.x_y-z", "0abc", "a" * 64])
def test_name_accepts(name):
    assert rw.worktree_path(ROOT, name) == f"{WT}/{name}"


@pytest.mark.parametrize(
    "name",
    ["", "A", "-a", ".a", "_a", "a" * 65, "a/b", "../x", "a..b", "a b", "a\n", "a.", "x.lock",
     "a;rm", "a$(x)", "é", None, 3],
)  # fmt: skip
def test_name_rejects(name):
    with pytest.raises(ValueError):
        rw.validate_name(name)


@pytest.mark.parametrize(
    "ref", ["origin/dev", "dev", "cl/huangrunhao/topic", "v1.2.3", "a" * 40, "refs/remotes/origin/dev"]
)  # fmt: skip
def test_ref_accepts(ref):
    assert rw.validate_ref(ref) == ref


@pytest.mark.parametrize(
    "ref",
    ["", "-x", "--detach", "-b", "a..b", "a b", "a~1", "a^", "a:b", "a?", "a*", "a[", "a\\b",
     "a@{1}", "@", "/a", "a/", "a//b", "a.", ".a", "a/.b", "a.lock", "a/b.lock/c", "a\n", "a\x00",
     "x" * 201, None],
)  # fmt: skip
def test_ref_rejects(ref):
    with pytest.raises(ValueError):
        rw.validate_ref(ref)


@pytest.mark.parametrize("branch", ["HEAD", "refs/heads/x", "a" * 40, "-b", "x..y"])
def test_branch_rejects(branch):
    with pytest.raises(ValueError):
        rw.validate_branch(branch)


def test_argv_forms_are_fixed():
    p = f"{WT}/t1"
    assert rw.add_argv(ROOT, p, "origin/dev", None) == [
        "git", "-C", ROOT, "worktree", "add", "--quiet", "--detach", "--", p, "origin/dev"
    ]  # fmt: skip
    assert rw.add_argv(ROOT, p, "origin/dev", "cl/x/y") == [
        "git", "-C", ROOT, "worktree", "add", "--quiet", "-b", "cl/x/y", "--no-track", "--", p, "origin/dev"
    ]  # fmt: skip
    assert rw.remove_argv(ROOT, p, False) == ["git", "-C", ROOT, "worktree", "remove", "--", p]
    assert rw.remove_argv(ROOT, p, True)[4:6] == ["remove", "--force"]
    assert rw.list_argv(ROOT)[3:] == ["worktree", "list", "--porcelain"]


def test_sandbox_is_minimal():
    policy = rw.write_sandbox(ROOT)
    assert policy["type"] == "workspaceWrite"
    assert policy["writableRoots"] == ["/code/.git", WT]
    assert policy["networkAccess"] is False
    assert policy["excludeSlashTmp"] and policy["excludeTmpdirEnvVar"]


def test_parse_porcelain():
    text = (
        "worktree /code\nHEAD aaa\nbranch refs/heads/dev\n\n"
        "worktree /code/.worktrees/t1\nHEAD bbb\ndetached\nlocked reason here\n\n"
    )
    entries = rw.parse_porcelain(text, ROOT)
    assert [e["managed"] for e in entries] == [False, True]
    assert entries[0]["branch"] == "refs/heads/dev"
    assert entries[1]["detached"] and entries[1]["locked"] == "reason here"


# --- create_worktree_thread ----------------------------------------------------------------------
async def test_create_detached_runs_exec_then_starts_thread(remote):
    fake, bridge = remote
    receipt = await bridge.create_remote_worktree_thread(
        "c1", "smoke", "origin/dev", "read-only", prompt="check", title="smoke"
    )
    assert receipt["status"] == "accepted", receipt
    add, head = execs(fake)
    assert add["command"] == [
        "git", "-C", ROOT, "worktree", "add", "--quiet", "--detach", "--", f"{WT}/smoke", "origin/dev"
    ]  # fmt: skip
    assert add["cwd"] == ROOT
    assert add["sandboxPolicy"] == rw.write_sandbox(ROOT)
    assert add["timeoutMs"] > 0 and add["processId"].startswith("ctb-")
    assert add["env"]["GIT_TERMINAL_PROMPT"] == "0"
    assert head["sandboxPolicy"] == rw.READ_SANDBOX
    start = next(p for m, p in fake.calls if m == "thread/start")
    assert start["cwd"] == f"{WT}/smoke" and start["sandbox"] == "read-only"
    assert start["approvalPolicy"] == "never"
    assert start["runtimeWorkspaceRoots"] == [f"{WT}/smoke"]
    turn = next(p for m, p in fake.calls if m == "turn/start")
    assert turn["input"] == [{"type": "text", "text": "check"}]
    assert receipt["worktree"]["state"] == "created" and receipt["worktree"]["head"] == "a" * 40
    assert receipt["threadId"] and receipt["turnId"]


async def test_create_with_branch(remote):
    fake, bridge = remote
    receipt = await bridge.create_remote_worktree_thread(
        "c2", "t2", "origin/dev", "workspace-write", branch="cl/huangrunhao/topic"
    )
    assert receipt["status"] == "accepted"
    assert execs(fake)[0]["command"][6:9] == ["-b", "cl/huangrunhao/topic", "--no-track"]
    assert fake.count("turn/start") == 0  # no prompt, no turn


@pytest.mark.parametrize(
    "kwargs",
    [
        {"name": "../escape"},
        {"name": "Bad"},
        {"name": ""},
        {"base_ref": "-x"},
        {"base_ref": "a b"},
        {"base_ref": "origin/dev; rm -rf /"},
        {"branch": "--force"},
        {"branch": "a..b"},
        {"branch": "HEAD"},
        {"sandbox": "danger-full-access"},
        {"prompt": " "},
    ],
)
async def test_create_rejects_invalid_input_before_any_call(remote, kwargs):
    fake, bridge = remote
    args = {"name": "ok", "base_ref": "origin/dev", "sandbox": "read-only", **kwargs}
    with pytest.raises(ValueError):
        await bridge.create_remote_worktree_thread("bad", **args)
    assert fake.calls == []


async def test_create_git_failure_reports_exit_code_and_skips_thread(remote):
    fake, bridge = remote
    fake.exec_fail["worktree add"] = (128, "fatal: invalid reference: origin/nope")
    receipt = await bridge.create_remote_worktree_thread(
        "c3", "t3", "origin/nope", "read-only", prompt="x"
    )
    assert receipt["status"] == "failed"
    assert "exit 128" in receipt["error"] and "invalid reference" in receipt["error"]
    assert receipt["worktree"]["add"]["exitCode"] == 128
    assert receipt["worktree"]["add"]["stderr"].startswith("fatal")
    assert fake.count("thread/start") == 0


async def test_create_replays_receipt_without_rerunning_git(remote):
    fake, bridge = remote
    first = await bridge.create_remote_worktree_thread("c4", "t4", "origin/dev", "read-only")
    again = await bridge.create_remote_worktree_thread("c4", "t4", "origin/dev", "read-only")
    assert again["replayed"] and again["threadId"] == first["threadId"]
    assert len(execs(fake)) == 2  # add + rev-parse only once


SETUP = "/agent-context/scripts/sync-charm-context"


@pytest.fixture
async def remote_with_setup(fake_server, tmp_path):
    fake, socket = fake_server
    rpc = AppServer(socket, timeout=2, decline_server_requests=True, retry_reads=True)
    ledger = Ledger(tmp_path / "state" / "operations.sqlite3")
    try:
        yield fake, Bridge(rpc, ledger, remote_root=ROOT, worktree_setup=SETUP)
    finally:
        await rpc.close()
        ledger.close()


async def test_create_runs_setup_after_checkout_before_thread(remote_with_setup):
    fake, bridge = remote_with_setup
    receipt = await bridge.create_remote_worktree_thread(
        "s1", "ctx", "origin/dev", "workspace-write", prompt="go"
    )
    assert receipt["status"] == "accepted", receipt
    add, head, setup = execs(fake)
    assert setup["command"] == [SETUP, f"{WT}/ctx"]
    assert setup["sandboxPolicy"] == rw.write_sandbox(ROOT)
    assert receipt["worktree"]["setup"]["exitCode"] == 0
    methods = [m for m, _ in fake.calls]
    assert methods.index("thread/start") > max(
        i for i, m in enumerate(methods) if m == "command/exec"
    )


async def test_setup_failure_withholds_task(remote_with_setup):
    fake, bridge = remote_with_setup
    fake.exec_fail["setup"] = (1, "sync-charm-context: refusing tracked Charm path(s)")
    receipt = await bridge.create_remote_worktree_thread(
        "s2", "ctx2", "origin/dev", "workspace-write", prompt="go"
    )
    assert receipt["status"] == "failed"
    assert "worktree setup failed (exit 1)" in receipt["error"]
    assert receipt["worktree"]["setup"]["stderr"].startswith("sync-charm-context")
    assert receipt["worktree"]["state"] == "created"  # retained for inspection
    assert fake.count("thread/start") == 0 and fake.count("turn/start") == 0


async def test_without_setup_no_extra_exec(remote):
    fake, bridge = remote
    await bridge.create_remote_worktree_thread("s3", "plain", "origin/dev", "read-only")
    assert len(execs(fake)) == 2


@pytest.mark.parametrize(
    "remote_root, setup", [(None, SETUP), (ROOT, "relative/setup"), (ROOT, "/a/../b")]
)
async def test_setup_path_validation(fake_server, tmp_path, remote_root, setup):
    _, socket = fake_server
    rpc = AppServer(socket, timeout=2)
    ledger = Ledger(tmp_path / "state" / "operations.sqlite3")
    try:
        with pytest.raises(ValueError, match="worktree_setup"):
            Bridge(rpc, ledger, remote_root=remote_root, worktree_setup=setup)
    finally:
        await rpc.close()
        ledger.close()


async def test_local_mode_has_no_remote_worktree_tools(bridge):
    with pytest.raises(ValueError, match="only in remote mode"):
        await bridge.create_remote_worktree_thread("x", "a", "origin/dev", "read-only")
    with pytest.raises(ValueError, match="only in remote mode"):
        await bridge.remove_worktree("a")
    with pytest.raises(ValueError, match="only in remote mode"):
        await bridge.list_worktrees()


# --- list / remove -------------------------------------------------------------------------------
async def test_list_worktrees(remote):
    fake, bridge = remote
    await bridge.create_remote_worktree_thread("l1", "t1", "origin/dev", "read-only", branch="b1")
    listing = await bridge.list_worktrees()
    assert listing["status"] == "ok"
    managed = [w for w in listing["worktrees"] if w["managed"]]
    assert [w["path"] for w in managed] == [f"{WT}/t1"]
    assert managed[0]["branch"] == "refs/heads/b1"
    last = execs(fake)[-1]
    assert last["command"] == ["git", "-C", ROOT, "worktree", "list", "--porcelain"]
    assert last["sandboxPolicy"] == rw.READ_SANDBOX


async def test_list_failure_returns_exec_output(remote):
    fake, bridge = remote
    fake.exec_fail["worktree list"] = (128, "fatal: not a git repository")
    result = await bridge.list_worktrees()
    assert result["status"] == "failed" and result["command"]["exitCode"] == 128


async def test_remove_worktree_without_force(remote):
    fake, bridge = remote
    await bridge.create_remote_worktree_thread("r1", "t1", "origin/dev", "read-only")
    result = await bridge.remove_worktree("t1")
    assert result["status"] == "removed" and result["forced"] is False
    removal = execs(fake)[-1]
    assert removal["command"] == ["git", "-C", ROOT, "worktree", "remove", "--", f"{WT}/t1"]
    assert removal["sandboxPolicy"] == rw.write_sandbox(ROOT)
    assert (await bridge.list_worktrees())["worktrees"][-1]["path"] == ROOT


async def test_remove_worktree_force_is_explicit(remote):
    fake, bridge = remote
    await bridge.create_remote_worktree_thread("r2", "t2", "origin/dev", "read-only")
    result = await bridge.remove_worktree("t2", force=True)
    assert result["forced"] is True and "--force" in execs(fake)[-1]["command"]
    with pytest.raises(ValueError):
        await bridge.remove_worktree("t2", force="yes")


@pytest.mark.parametrize("name", ["../../etc", "a/b", "-f", "UP", "", "x" * 65])
async def test_remove_rejects_bad_names_without_exec(remote, name):
    fake, bridge = remote
    with pytest.raises(ValueError):
        await bridge.remove_worktree(name)
    assert fake.calls == []


async def test_remove_refuses_unregistered_path(remote):
    fake, bridge = remote
    with pytest.raises(ValueError, match="not a registered worktree"):
        await bridge.remove_worktree("never-created")
    assert all(c["command"][4] == "list" for c in execs(fake))  # only the listing ran


async def test_remove_failure_reports_exit_code(remote):
    fake, bridge = remote
    await bridge.create_remote_worktree_thread("r3", "t3", "origin/dev", "read-only")
    fake.exec_fail["worktree remove"] = (128, "fatal: contains modified or untracked files")
    result = await bridge.remove_worktree("t3")
    assert result["status"] == "failed"
    assert result["command"]["exitCode"] == 128
    assert "modified or untracked" in result["command"]["stderr"]


# --- MCP surface ---------------------------------------------------------------------------------
async def test_remote_mcp_tool_set(fake_server, tmp_path):
    _, socket = fake_server
    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "codex_thread_bridge.server", "--socket", str(socket), "--transport", "unix",
              "--state-dir", str(tmp_path / "state"), "--remote-root", ROOT],
    )  # fmt: skip
    async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
        await session.initialize()
        tools = {t.name: t for t in (await session.list_tools()).tools}
        assert {"remove_worktree", "list_worktrees", "create_worktree_thread"} <= set(tools)
        assert not any("exec" in n for n in tools)  # no generic command tool
        props = set(tools["create_worktree_thread"].inputSchema["properties"])
        assert {"name", "base_ref", "branch"} <= props and "destination" not in props
        result = await session.call_tool("list_worktrees", {})
        assert not result.isError
        assert json.loads(result.content[0].text)["status"] == "ok"
