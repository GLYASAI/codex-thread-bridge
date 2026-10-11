"""Remote mode: question-type server requests are held for answer_request."""

import asyncio
import subprocess
import sys
import time

import pytest
from mcp.shared.memory import create_connected_server_and_client_session as connected

from codex_thread_bridge.bridge import Bridge
from codex_thread_bridge.ledger import Ledger
from codex_thread_bridge.rpc import AppServer
from codex_thread_bridge.server import make_server

ROOT = "/code"
USER_INPUT = (
    "item/tool/requestUserInput",
    {
        "itemId": "item-1",
        "isBlocking": True,
        "questions": [
            {"id": "q1", "header": "Scope", "question": "Which module?", "options": None},
            {"id": "q2", "header": "Mode", "question": "Dry run?", "isOther": True},
        ],
    },
)
FORM = (
    "mcpServer/elicitation/request",
    {
        "serverName": "docs",
        "mode": "form",
        "message": "Pick a region",
        "requestedSchema": {
            "type": "object",
            "properties": {"region": {"type": "string"}, "tier": {"type": "string"}},
            "required": ["region"],
        },
    },
)
ANSWERS = {"q1": "auth", "q2": ["yes"]}


def make_bridge(socket, tmp_path, request_timeout=600, **kwargs):
    rpc = AppServer(
        socket,
        timeout=2,
        decline_server_requests=True,
        retry_reads=True,
        declined_log=tmp_path / "state" / "declined.jsonl",
        request_timeout=request_timeout,
    )
    ledger = Ledger(tmp_path / "state" / "operations.sqlite3")
    return rpc, ledger, Bridge(rpc, ledger, remote_root=ROOT, **kwargs)


@pytest.fixture
async def held(fake_server, tmp_path):
    fake, socket = fake_server
    rpc, ledger, bridge = make_bridge(socket, tmp_path)
    try:
        yield fake, bridge, rpc
    finally:
        await rpc.close()
        ledger.close()


async def until(condition, timeout=3.0):
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "condition not reached"
        await asyncio.sleep(0.02)


async def settle(bridge, thread_id, turn_id):
    """Wait until the turn is terminal, even across early returns for pending requests."""
    for _ in range(50):
        waited = await bridge.wait_thread(thread_id, turn_id, 1)
        if not waited["pendingRequests"] and not waited["timedOut"]:
            return waited
        await asyncio.sleep(0.1)
    raise AssertionError("turn never settled")


async def start(bridge, request_id="c1"):
    return await bridge.create_thread(request_id, "/code/x", "work", sandbox="read-only")


async def test_question_is_held_and_listed_everywhere(held):
    fake, bridge, rpc = held
    fake.server_requests = [USER_INPUT]
    created = await start(bridge)
    thread_id, turn_id = created["threadId"], created["turnId"]
    waited = await bridge.wait_thread(thread_id, turn_id, 0)
    assert waited["turn"]["status"] == "inProgress"
    assert waited["waitingForAnswer"] is True and waited["timedOut"] is False
    (pending,) = waited["pendingRequests"]
    assert pending["type"] == "userInput" and pending["method"] == USER_INPUT[0]
    assert pending["threadId"] == thread_id and pending["turnId"] == turn_id
    assert pending["requestId"].startswith("rq-")
    assert [q["id"] for q in pending["request"]["questions"]] == ["q1", "q2"]
    assert pending["deadlineAt"] > time.time() and 0 < pending["secondsRemaining"] <= 600
    assert fake.client_replies == [], "a held request must not be answered"
    read = await bridge.read_thread(thread_id)
    assert [p["requestId"] for p in read["pendingRequests"]] == [pending["requestId"]]
    result = await bridge.get_turn_result(thread_id, turn_id)
    assert [p["requestId"] for p in result["pendingRequests"]] == [pending["requestId"]]
    assert result["declinedRequests"] == []
    caps = await bridge.capabilities()
    assert caps["pendingRequests"]["held"] == 1 and caps["pendingRequests"]["timeoutSeconds"] == 600


async def test_answer_user_input_completes_the_turn(held):
    fake, bridge, rpc = held
    fake.server_requests = [USER_INPUT]
    created = await start(bridge)
    (pending,) = rpc.pending_for(created["threadId"])
    sent = await bridge.answer_request(pending["requestId"], ANSWERS)
    assert sent["sent"] == "answers" and sent["turnId"] == created["turnId"]
    waited = await bridge.wait_thread(created["threadId"], created["turnId"], 5)
    assert waited["turn"]["status"] == "completed" and waited["pendingRequests"] == []
    (reply,) = fake.client_replies
    assert reply["result"] == {"answers": {"q1": {"answers": ["auth"]}, "q2": {"answers": ["yes"]}}}
    assert rpc.held == {}
    result = await bridge.get_turn_result(created["threadId"], created["turnId"])
    assert result["declinedRequests"] == [] and result["pendingRequests"] == []
    with pytest.raises(ValueError, match="No pending request"):
        await bridge.answer_request(pending["requestId"], ANSWERS)


async def test_answer_validation_leaves_the_request_pending(held):
    fake, bridge, rpc = held
    fake.server_requests = [USER_INPUT]
    created = await start(bridge)
    (pending,) = rpc.pending_for(created["threadId"])
    rid = pending["requestId"]
    bad = [
        ({"q1": "a"}, False, "Missing answer"),
        ({"q1": "a", "q2": "b", "q3": "c"}, False, "Unknown question"),
        ({"q1": "a", "q2": ""}, False, "non-empty"),
        ({"q1": "a", "q2": []}, False, "non-empty"),
        ({"q1": "a", "q2": 3}, False, "non-empty"),
        ({}, False, "map each question id"),
        (None, False, "answers"),
        (ANSWERS, True, "not both"),
    ]
    for answers, decline, message in bad:
        with pytest.raises(ValueError, match=message):
            await bridge.answer_request(rid, answers, decline)
    with pytest.raises(ValueError, match="No pending request"):
        await bridge.answer_request("rq-nope", ANSWERS)
    assert fake.client_replies == [] and rid in rpc.held


async def test_decline_user_input(held):
    fake, bridge, rpc = held
    fake.server_requests = [USER_INPUT]
    created = await start(bridge)
    (pending,) = rpc.pending_for(created["threadId"])
    sent = await bridge.answer_request(pending["requestId"], decline=True)
    assert sent["sent"] == "decline"
    waited = await bridge.wait_thread(created["threadId"], created["turnId"], 5)
    assert waited["turn"]["status"] == "completed"
    assert [r["result"] for r in fake.client_replies] == [{"answers": {}}]


async def test_elicitation_accept_decline_and_validation(held):
    fake, bridge, rpc = held
    fake.server_requests = [FORM]
    created = await start(bridge)
    (pending,) = rpc.pending_for(created["threadId"])
    assert pending["type"] == "elicitation" and pending["request"]["serverName"] == "docs"
    rid = pending["requestId"]
    with pytest.raises(ValueError, match="Missing required"):
        await bridge.answer_request(rid, {"tier": "x"})
    with pytest.raises(ValueError, match="Unknown field"):
        await bridge.answer_request(rid, {"region": "eu", "bogus": 1})
    await bridge.answer_request(rid, {"region": "eu"})
    waited = await bridge.wait_thread(created["threadId"], created["turnId"], 5)
    assert waited["turn"]["status"] == "completed"
    assert [r["result"] for r in fake.client_replies] == [
        {"action": "accept", "content": {"region": "eu"}}
    ]

    fake.client_replies.clear()
    fake.server_requests = [FORM]
    again = await start(bridge, "c2")
    (second,) = rpc.pending_for(again["threadId"])
    await bridge.answer_request(second["requestId"], decline=True)
    await bridge.wait_thread(again["threadId"], again["turnId"], 5)
    assert [r["result"] for r in fake.client_replies] == [{"action": "decline"}]


async def test_elicitation_url_and_user_verification_modes(held):
    fake, bridge, rpc = held
    url = (
        "mcpServer/elicitation/request",
        {
            "serverName": "s",
            "mode": "url",
            "message": "m",
            "url": "https://x",
            "elicitationId": "e1",
        },
    )
    verify = (
        "mcpServer/elicitation/request",
        {
            "serverName": "s",
            "mode": "openai/userVerification",
            "title": "t",
            "description": "d",
            "challenge": "c",
        },
    )
    fake.server_requests = [url]
    first = await start(bridge)
    (pending,) = rpc.pending_for(first["threadId"])
    with pytest.raises(ValueError, match="answers=\\{\\}"):
        await bridge.answer_request(pending["requestId"], {"x": 1})
    await bridge.answer_request(pending["requestId"], {})
    fake.server_requests = [verify]
    second = await start(bridge, "c2")
    (pending,) = rpc.pending_for(second["threadId"])
    with pytest.raises(ValueError, match="device-authenticated"):
        await bridge.answer_request(pending["requestId"], {"proof": "x"})
    await bridge.answer_request(pending["requestId"], decline=True)
    await until(lambda: len(fake.client_replies) == 2)
    assert [r["result"] for r in fake.client_replies] == [
        {"action": "accept"},
        {"action": "decline"},
    ]


async def test_timeout_auto_declines_and_records(fake_server, tmp_path):
    fake, socket = fake_server
    rpc, ledger, bridge = make_bridge(socket, tmp_path, request_timeout=0.3)
    try:
        fake.server_requests = [USER_INPUT]
        created = await start(bridge)
        assert rpc.pending_for(created["threadId"])
        (pending,) = rpc.pending_for(created["threadId"])
        waited = await settle(bridge, created["threadId"], created["turnId"])
        assert waited["turn"]["status"] == "completed", "timeout must release the turn"
        assert [r["result"] for r in fake.client_replies] == [{"answers": {}}]
        assert rpc.held == {}
        result = await bridge.get_turn_result(created["threadId"], created["turnId"])
        (record,) = result["declinedRequests"]
        assert record["category"] == "question" and "timed out" in record["answer"]
        assert record["detail"] == "Which module?"
        assert result["pendingRequests"] == []
        with pytest.raises(ValueError, match="timed out"):
            await bridge.answer_request(pending["requestId"], ANSWERS)
        read = await bridge.read_thread(created["threadId"])
        assert len(read["declinedRequests"]) == 1
    finally:
        await rpc.close()
        ledger.close()


async def test_zero_timeout_declines_immediately(fake_server, tmp_path):
    fake, socket = fake_server
    rpc, ledger, bridge = make_bridge(socket, tmp_path, request_timeout=0)
    try:
        fake.server_requests = [USER_INPUT, FORM]
        created = await start(bridge)
        waited = await bridge.wait_thread(created["threadId"], created["turnId"], 5)
        assert waited["turn"]["status"] == "completed"
        assert [r["result"] for r in fake.client_replies] == [
            {"answers": {}},
            {"action": "decline"},
        ]
    finally:
        await rpc.close()
        ledger.close()


async def test_blocked_wait_returns_as_soon_as_a_request_is_pending(held):
    fake, bridge, rpc = held
    fake.complete_turns = False
    created = await start(bridge)
    thread_id, turn_id = created["threadId"], created["turnId"]
    started = time.monotonic()
    waiter = asyncio.create_task(bridge.wait_thread(thread_id, turn_id, 30))
    await asyncio.sleep(0.4)
    assert not waiter.done()
    message = {
        "id": "srv-x",
        "method": USER_INPUT[0],
        "params": {"threadId": thread_id, "turnId": turn_id, **USER_INPUT[1]},
    }
    await rpc._answer_server_request(rpc._ws, message)
    waited = await asyncio.wait_for(waiter, 3)
    assert time.monotonic() - started < 3
    assert waited["waitingForAnswer"] is True and waited["timedOut"] is False
    assert len(waited["pendingRequests"]) == 1
    # a pending request for another turn does not wake this wait
    other = await bridge.wait_thread(thread_id, "turn-other", 0)
    assert other["pendingRequests"] == []


async def test_two_questions_in_one_turn_and_approval_still_declined(held):
    fake, bridge, rpc = held
    fake.server_requests = [
        ("item/commandExecution/requestApproval", {"command": "rm -rf /"}),
        USER_INPUT,
        FORM,
    ]
    created = await start(bridge)
    thread_id, turn_id = created["threadId"], created["turnId"]
    # the approval is declined at once; both questions stay held
    await until(lambda: len(fake.client_replies) == 1)
    assert [r["result"] for r in fake.client_replies] == [{"decision": "decline"}]
    first, second = rpc.pending_for(thread_id, turn_id)
    assert {first["type"], second["type"]} == {"userInput", "elicitation"}
    result = await bridge.get_turn_result(thread_id, turn_id)
    assert [r["category"] for r in result["declinedRequests"]] == ["approval"]
    assert len(result["pendingRequests"]) == 2
    await bridge.answer_request(first["requestId"], decline=True)
    mid = await bridge.wait_thread(thread_id, turn_id, 0)
    assert mid["turn"]["status"] == "inProgress" and len(mid["pendingRequests"]) == 1
    await bridge.answer_request(second["requestId"], decline=True)
    done = await bridge.wait_thread(thread_id, turn_id, 5)
    assert done["turn"]["status"] == "completed"


async def test_turn_end_and_resolved_notifications_clean_up(held):
    fake, bridge, rpc = held
    fake.server_requests = [USER_INPUT, FORM]
    created = await start(bridge)
    thread_id, turn_id = created["threadId"], created["turnId"]
    first, second = rpc.pending_for(thread_id)
    rpc._observe_notification(
        {
            "method": "serverRequest/resolved",
            "params": {"threadId": thread_id, "requestId": first["requestId"]},
        }
    )
    assert len(rpc.held) == 2, "resolved matches the JSON-RPC id, not the bridge request id"
    rpc_id = rpc.held[first["requestId"]]["_rpc_id"]
    rpc._observe_notification(
        {"method": "serverRequest/resolved", "params": {"threadId": thread_id, "requestId": rpc_id}}
    )
    assert list(rpc.held) == [second["requestId"]]
    rpc._observe_notification(
        {"method": "turn/completed", "params": {"threadId": "other", "turn": {"id": turn_id}}}
    )
    assert len(rpc.held) == 1
    rpc._observe_notification(
        {"method": "turn/completed", "params": {"threadId": thread_id, "turn": {"id": turn_id}}}
    )
    assert rpc.held == {}
    answers = [r["answer"] for r in rpc.declined_for(thread_id, turn_id)]
    assert any("resolved by the App Server" in a for a in answers)
    assert any("turn ended" in a for a in answers)


async def test_reconnect_drops_held_requests(held):
    fake, bridge, rpc = held
    fake.server_requests = [USER_INPUT]
    created = await start(bridge)
    (pending,) = rpc.pending_for(created["threadId"])
    await rpc.close()
    assert rpc.held == {}
    with pytest.raises(ValueError, match="bridge restarted"):
        await bridge.answer_request(pending["requestId"], ANSWERS)
    (record,) = rpc.declined_for(created["threadId"], created["turnId"])
    assert "connection ended" in record["answer"]


async def test_unknown_requests_still_get_method_not_found(held):
    fake, bridge, rpc = held
    fake.server_requests = [("account/chatgptAuthTokens/refresh", {})]
    created = await start(bridge)
    await bridge.wait_thread(created["threadId"], created["turnId"], 5)
    assert fake.client_replies[0]["error"]["code"] == -32601
    assert rpc.held == {}
    result = await bridge.get_turn_result(created["threadId"], created["turnId"])
    assert result["declinedRequests"][0]["category"] == "unsupported"


async def test_default_mode_ignores_questions(fake_server, tmp_path):
    fake, socket = fake_server
    fake.server_requests = [USER_INPUT]
    rpc = AppServer(socket, timeout=1)
    bridge = Bridge(rpc, Ledger(tmp_path / "s" / "o.sqlite3"))
    try:
        await bridge.create_thread("u1", str(tmp_path), "work")
        await asyncio.sleep(0.2)
        assert fake.client_replies == [] and rpc.held == {}
    finally:
        await rpc.close()


async def test_two_mcp_sessions_share_pending_requests(held):
    fake, bridge, rpc = held
    fake.server_requests = [USER_INPUT]
    server = make_server(bridge, http=True)._mcp_server
    async with connected(server) as one, connected(server) as two:
        tools = {tool.name for tool in (await two.list_tools()).tools}
        assert "answer_request" in tools and len(tools) == 16
        created = await one.call_tool(
            "create_thread",
            {"request_id": "m1", "cwd": "/code/x", "prompt": "work", "sandbox": "read-only"},
        )
        thread_id = created.structuredContent["threadId"]
        turn_id = created.structuredContent["turnId"]
        seen = await two.call_tool("wait_thread", {"thread_id": thread_id, "turn_id": turn_id})
        (pending,) = seen.structuredContent["pendingRequests"]
        bad = await two.call_tool(
            "answer_request", {"request_id": pending["requestId"], "answers": {"q1": "only one"}}
        )
        assert bad.isError
        ok = await two.call_tool(
            "answer_request", {"request_id": pending["requestId"], "answers": ANSWERS}
        )
        assert not ok.isError and ok.structuredContent["sent"] == "answers"
        done = await one.call_tool(
            "wait_thread", {"thread_id": thread_id, "turn_id": turn_id, "timeout_seconds": 5}
        )
        assert done.structuredContent["turn"]["status"] == "completed"
        late = await one.call_tool(
            "answer_request", {"request_id": pending["requestId"], "answers": ANSWERS}
        )
        assert late.isError


async def test_answer_request_tool_only_in_remote_mode(bridge):
    async with connected(make_server(bridge)._mcp_server) as session:
        names = {tool.name for tool in (await session.list_tools()).tools}
    assert "answer_request" not in names and len(names) == 13


async def test_capabilities_report_the_http_transport(fake_server, tmp_path):
    _, socket = fake_server
    rpc, ledger, bridge = make_bridge(
        socket, tmp_path, mcp_transport="streamable-http on unix socket /run/b.sock"
    )
    try:
        caps = await bridge.capabilities()
        assert caps["mcpTransport"] == "streamable-http on unix socket /run/b.sock"
        assert "streamable-http on unix socket /run/b.sock" in caps["transport"]
        assert str(socket) in caps["transport"]
        assert "ssh-forwarded" not in caps["transport"]
    finally:
        await rpc.close()
        ledger.close()


def test_request_timeout_flag():
    base = [sys.executable, "-m", "codex_thread_bridge.server"]
    help_text = subprocess.run([*base, "--help"], capture_output=True, text=True, timeout=30).stdout
    assert "--request-timeout SECONDS" in help_text and "default 600" in help_text
    bad = subprocess.run(
        [*base, "--request-timeout", "-1"], capture_output=True, text=True, timeout=30
    )
    assert bad.returncode == 2 and "--request-timeout must be >= 0" in bad.stderr
