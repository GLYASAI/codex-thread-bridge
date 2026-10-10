"""Remote mode: no local filesystem checks, repo-root confinement, new tools, declined requests."""

import asyncio
import json

import pytest

from codex_thread_bridge.bridge import Bridge, remote_directory
from codex_thread_bridge.ledger import Ledger, open_endpoint_ledger
from codex_thread_bridge.rpc import AppServer, RpcError

ROOT = "/code"  # deliberately absent on the machine running the tests


@pytest.fixture
async def remote(fake_server, tmp_path):
    fake, socket = fake_server
    rpc = AppServer(
        socket,
        timeout=2,
        decline_server_requests=True,
        declined_log=tmp_path / "state" / "declined.jsonl",
    )
    ledger = Ledger(tmp_path / "state" / "operations.sqlite3")
    try:
        yield fake, Bridge(rpc, ledger, remote_root=ROOT), rpc
    finally:
        await rpc.close()
        ledger.close()


@pytest.mark.parametrize(
    "path",
    ["/code", "/code/.worktrees/task-1", "/code/a/b.c"],
)
def test_remote_directory_accepts_paths_under_root(path):
    assert remote_directory(path, ROOT) == path


@pytest.mark.parametrize(
    "path",
    [
        "",
        "code",
        "relative/dir",
        "/",
        "/etc",
        "/codex",  # prefix but not a path component
        "/code/../etc",
        "/code/a/../../etc",
        "/code/./a",
        "/code//a",
        "/code/a/",
        "/code\n/a",
        None,
    ],
)
def test_remote_directory_rejects_other_paths(path):
    with pytest.raises(ValueError):
        remote_directory(path, ROOT)


def test_remote_root_must_be_clean_absolute(fake_server, tmp_path):
    for bad in ("code", "/", "/code/", "/code/../x"):
        with pytest.raises(ValueError):
            Bridge(None, None, remote_root=bad)


async def test_create_thread_skips_local_checks_and_requires_root(remote):
    fake, bridge, _ = remote
    receipt = await bridge.create_thread(
        "r1", "/code/.worktrees/t1", "do it", sandbox="workspace-write"
    )
    assert receipt["status"] == "accepted", receipt
    assert fake.calls[-1][0] == "turn/start" or fake.count("thread/start") == 1
    start = next(p for m, p in fake.calls if m == "thread/start")
    assert start["cwd"] == "/code/.worktrees/t1"
    assert start["approvalPolicy"] == "never"
    for bad in ("/etc", "/code/../etc", "relative"):
        with pytest.raises(ValueError):
            await bridge.create_thread("r-bad", bad, "x")
    assert fake.count("thread/start") == 1


async def test_remote_sandbox_and_approval_allowlist(remote):
    _, bridge, _ = remote
    with pytest.raises(ValueError, match="read-only or workspace-write"):
        await bridge.create_thread("r2", "/code/x", "p", sandbox="danger-full-access")
    with pytest.raises(ValueError, match="approval_policy never"):
        await bridge.create_thread("r3", "/code/x", "p", approval_policy="on-request")
    with pytest.raises(ValueError, match="readOnly or workspaceWrite"):
        await bridge.update_thread_permissions(
            "r4",
            "thread-1",
            {"type": "dangerFullAccess"},
            {"thread_id": "thread-1", "cwd": "/code/x", "model": "m", "reasoning_effort": "low"},
        )


async def test_remote_writable_roots_must_be_under_root(remote):
    _, bridge, _ = remote
    policy = {
        "type": "workspaceWrite",
        "networkAccess": False,
        "writableRoots": ["/tmp/elsewhere"],
        "excludeTmpdirEnvVar": False,
        "excludeSlashTmp": False,
    }
    with pytest.raises(ValueError, match="under the configured repository root"):
        await bridge.create_thread(
            "r5", "/code/x", "p", sandbox="workspace-write", sandbox_policy=policy
        )


async def test_create_worktree_thread_is_disabled_in_remote_mode(remote):
    fake, bridge, _ = remote
    with pytest.raises(ValueError, match="disabled in remote mode.*command/exec"):
        await bridge.create_worktree_thread(
            "w1", "/code", "a" * 40, "/code/.worktrees/w", "bridge-managed-retained",
            "read-only", {"type": "readOnly", "networkAccess": False},
        )  # fmt: skip
    assert fake.calls == []


async def test_list_threads_cwd_filter_is_remote_checked(remote):
    fake, bridge, _ = remote
    await bridge.list_threads("/code/x")
    assert next(p for m, p in fake.calls if m == "thread/list")["cwd"] == "/code/x"
    with pytest.raises(ValueError):
        await bridge.list_threads("/etc")


async def test_threads_outside_root_are_not_driven(remote):
    fake, bridge, _ = remote
    fake.threads["outside"] = {
        "id": "outside",
        "cwd": "/home/other",
        "status": {"type": "idle"},
        "turns": [{"id": "turn-1", "status": "inProgress", "items": []}],
    }
    with pytest.raises(ValueError, match="Refusing to drive"):
        await bridge.interrupt_turn("outside", "turn-1")
    with pytest.raises(ValueError, match="Refusing to drive"):
        await bridge.get_turn_result("outside", "turn-1")
    receipt = await bridge.steer_thread("s1", "outside", "turn-1", "hi")
    assert receipt["status"] == "outcome_unknown" or receipt["status"] == "failed"
    assert fake.count("turn/steer") == 0
    receipt = await bridge.send_message_to_thread("m1", "outside", "hi")
    assert receipt["status"] != "accepted"
    assert fake.count("turn/start") == 0


async def test_send_message_refuses_resumed_dangerous_sandbox(remote):
    fake, bridge, _ = remote
    created = await bridge.create_thread("c1", "/code/x", "first", sandbox="workspace-write")
    thread_id = created["threadId"]
    fake.settings[thread_id]["sandbox"] = {"type": "dangerFullAccess"}
    before = fake.count("turn/start")
    receipt = await bridge.send_message_to_thread("m2", thread_id, "again")
    assert receipt["status"] == "failed"
    assert "remote_sandbox_not_allowed" in json.dumps(receipt)
    assert fake.count("turn/start") == before


async def test_interrupt_turn(remote):
    fake, bridge, _ = remote
    fake.complete_turns = False
    created = await bridge.create_thread("c2", "/code/x", "long", sandbox="read-only")
    result = await bridge.interrupt_turn(created["threadId"], created["turnId"])
    assert result["interruptRequested"] is True
    assert (
        "turn/interrupt",
        {"threadId": created["threadId"], "turnId": created["turnId"]},
    ) in fake.calls
    waited = await bridge.wait_thread(created["threadId"], created["turnId"], 0)
    assert waited["turn"]["status"] == "interrupted"


async def test_get_turn_result_reports_message_diffs_and_commands(remote):
    fake, bridge, _ = remote
    fake.turn_items = [
        {
            "type": "commandExecution",
            "command": "pytest -q",
            "status": "completed",
            "exitCode": 0,
            "aggregatedOutput": "ok",
        },
        {
            "type": "fileChange",
            "id": "fc1",
            "status": "completed",
            "changes": [
                {"path": "/code/x/a.py", "kind": {"type": "update"}, "diff": "@@ -1 +1 @@"}
            ],
        },
        {"type": "agentMessage", "text": "All done."},
    ]
    created = await bridge.create_thread("c3", "/code/x", "work", sandbox="workspace-write")
    result = await bridge.get_turn_result(created["threadId"], created["turnId"])
    assert result["found"] and result["terminal"] and result["status"] == "completed"
    assert result["finalAgentMessage"] == "All done."
    assert result["fileChanges"] == [
        {
            "id": "fc1",
            "status": "completed",
            "changes": [
                {"path": "/code/x/a.py", "kind": {"type": "update"}, "diff": "@@ -1 +1 @@"}
            ],
        }
    ]
    assert result["commands"] == [{"command": "pytest -q", "status": "completed", "exitCode": 0}]
    assert result["declinedRequests"] == []
    missing = await bridge.get_turn_result(created["threadId"], "turn-nope")
    assert missing["found"] is False


async def test_get_turn_result_truncates_large_text(remote):
    fake, bridge, _ = remote
    fake.turn_items = [{"type": "agentMessage", "text": "z" * 5000}]
    created = await bridge.create_thread("c4", "/code/x", "work", sandbox="read-only")
    result = await bridge.get_turn_result(created["threadId"], created["turnId"], 100)
    assert "[truncated; original length" in result["finalAgentMessage"]
    with pytest.raises(ValueError):
        await bridge.get_turn_result(created["threadId"], created["turnId"], 10)


@pytest.mark.parametrize(
    ("method", "params", "expected"),
    [
        ("item/commandExecution/requestApproval", {"command": "rm -rf /"}, {"decision": "decline"}),
        ("item/fileChange/requestApproval", {"reason": "write"}, {"decision": "decline"}),
        ("execCommandApproval", {"command": ["ls"]}, {"decision": "denied"}),
        ("applyPatchApproval", {}, {"decision": "denied"}),
        ("item/tool/requestUserInput", {"questions": []}, {"answers": {}}),
        ("mcpServer/elicitation/request", {"serverName": "s"}, {"action": "decline"}),
    ],
)
async def test_server_requests_are_declined_and_turn_finishes(remote, method, params, expected):
    fake, bridge, rpc = remote
    fake.server_requests = [(method, params)]
    created = await bridge.create_thread("d1", "/code/x", "work", sandbox="workspace-write")
    waited = await bridge.wait_thread(created["threadId"], created["turnId"], 5)
    assert waited["turn"]["status"] == "completed", "turn hung on an unanswered server request"
    assert [r["result"] for r in fake.client_replies] == [expected]
    result = await bridge.get_turn_result(created["threadId"], created["turnId"])
    (record,) = result["declinedRequests"]
    assert record["method"] == method and record["turnId"] == created["turnId"]


async def test_unknown_server_request_gets_method_not_found(remote):
    fake, bridge, _ = remote
    fake.server_requests = [("account/chatgptAuthTokens/refresh", {})]
    created = await bridge.create_thread("d2", "/code/x", "work", sandbox="read-only")
    await bridge.wait_thread(created["threadId"], created["turnId"], 5)
    assert fake.client_replies[0]["error"]["code"] == -32601
    result = await bridge.get_turn_result(created["threadId"], created["turnId"])
    assert result["declinedRequests"][0]["answer"].startswith("unsupported")


async def test_declines_persist_across_bridge_restarts(remote, tmp_path):
    fake, bridge, rpc = remote
    fake.server_requests = [("item/commandExecution/requestApproval", {"command": "x"})]
    created = await bridge.create_thread("d3", "/code/x", "work", sandbox="read-only")
    await bridge.wait_thread(created["threadId"], created["turnId"], 5)
    fresh = AppServer(rpc.socket_path, declined_log=tmp_path / "state" / "declined.jsonl")
    assert len(fresh.declined_for(created["threadId"], created["turnId"])) == 1
    assert fresh.declined_for("other-thread") == []


async def test_default_mode_still_ignores_server_requests(fake_server, tmp_path):
    fake, socket = fake_server
    fake.server_requests = [("item/commandExecution/requestApproval", {"command": "x"})]
    rpc = AppServer(socket, timeout=1)
    bridge = Bridge(rpc, Ledger(tmp_path / "s" / "o.sqlite3"))
    try:
        await bridge.create_thread("u1", str(tmp_path), "work")
        await asyncio.sleep(0.2)
        assert fake.client_replies == []
    finally:
        await rpc.close()


async def test_capabilities_report_remote_mode(remote):
    _, bridge, _ = remote
    caps = await bridge.capabilities()
    assert caps["remoteRoot"] == ROOT
    assert caps["capabilities"]["bridgeManagedWorktrees"] is False
    assert caps["capabilities"]["clientSideToolsAndApprovals"] == "declined and recorded"
    assert caps["server"]["userAgent"]


async def test_non_remote_mode_still_checks_local_filesystem(bridge):
    with pytest.raises(ValueError, match="existing absolute directory"):
        await bridge.create_thread("l1", "/definitely/not/here", "p")


def test_endpoint_id_keys_the_ledger_not_the_socket_path(tmp_path):
    a_path, a = open_endpoint_ledger(tmp_path / "one" / "s.sock", tmp_path / "st", "charm:x")
    b_path, b = open_endpoint_ledger(tmp_path / "two" / "s.sock", tmp_path / "st", "charm:x")
    c_path, c = open_endpoint_ledger(tmp_path / "one" / "s.sock", tmp_path / "st", "charm:y")
    try:
        assert a_path != b_path
        a.begin("req", "m", {"k": 1})
        assert b.get("req")["requestId"] == "req"
        with pytest.raises(ValueError):
            c.get("req")
    finally:
        for ledger in (a, b, c):
            ledger.close()


def test_rpc_error_type_is_exported():
    assert issubclass(RpcError, Exception)
