"""A multiplexed JSON-RPC client over Codex's local WebSocket control socket."""

import asyncio
import collections
import contextlib
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any

from websockets.asyncio.client import unix_connect

from . import __version__
from .proxy_transport import ProxyWebSocket

# Remote mode (references: README "Remote mode"): server requests are answered instead of ignored,
# so a turn can never wait forever on a prompt nobody will answer.
# Approval-type requests are always declined at once. Question-type requests (HELD_METHODS) are held
# for answer_request until request_timeout, then declined. Anything not listed gets a JSON-RPC
# -32601 error, which also unblocks the turn.
DECLINE_RESULTS: dict[str, dict] = {
    "item/commandExecution/requestApproval": {"decision": "decline"},
    "item/fileChange/requestApproval": {"decision": "decline"},
    "item/permissions/requestApproval": {"permissions": {}, "scope": "turn"},
    "execCommandApproval": {"decision": "denied"},
    "applyPatchApproval": {"decision": "denied"},
    "item/tool/requestUserInput": {"answers": {}},
    "mcpServer/elicitation/request": {"action": "decline"},
}
HELD_METHODS = {
    "item/tool/requestUserInput": "userInput",
    "mcpServer/elicitation/request": "elicitation",
}
DEFAULT_REQUEST_TIMEOUT = 600.0
READ_ONLY_METHODS = frozenset(
    {"thread/read", "thread/list", "thread/turns/list", "thread/goal/get", "project/read"}
)
DECLINED_MEMORY = 200
DECLINED_LOG_TAIL_BYTES = 1_000_000


class RpcError(Exception):
    def __init__(self, method: str, error: dict[str, Any]):
        self.method = method
        self.error = error
        super().__init__(f"{method}: {error.get('message', error)}")


class TransportError(Exception):
    """A request may have reached the server. Mutations must not be retried."""


class AppServer:
    def __init__(
        self,
        socket_path: Path,
        timeout: float = 20,
        transport: str | None = None,
        codex_binary: str | None = None,
        decline_server_requests: bool = False,
        retry_reads: bool = False,
        declined_log: Path | None = None,
        request_timeout: float = DEFAULT_REQUEST_TIMEOUT,
    ):
        self.socket_path = socket_path
        self.timeout = timeout
        self.transport = transport or os.environ.get("CODEX_THREAD_BRIDGE_TRANSPORT", "auto")
        if self.transport not in {"auto", "unix", "proxy"}:
            raise ValueError("transport must be auto, unix, or proxy")
        self.codex_binary = codex_binary
        self._ws = None
        self._reader = None
        self._pending: dict[int, asyncio.Future] = {}
        self._counter = 0
        self._connect_lock = asyncio.Lock()
        self.info: dict[str, Any] = {}
        self.decline_server_requests = decline_server_requests
        self.retry_reads = retry_reads
        self.declined_log = declined_log
        self.declined: collections.deque = collections.deque(maxlen=DECLINED_MEMORY)
        # Held question-type requests, shared by every MCP session (one process, one event loop).
        # Lost when the bridge restarts or the App Server connection drops.
        self.request_timeout = request_timeout
        self.held: dict[str, dict[str, Any]] = {}
        self._held_changed: asyncio.Event | None = None

    @property
    def connection(self) -> str:
        """How this bridge reaches the App Server."""
        proxy = self.transport == "proxy" or (self.transport == "auto" and sys.platform == "win32")
        return "codex CLI proxy" if proxy else "unix WebSocket control socket"

    # --- held question-type requests -----------------------------------------------------------

    def _notify_held(self):
        if self._held_changed is not None:
            self._held_changed.set()
            self._held_changed = None

    async def wait_held_change(self, timeout: float):
        """Return when a request is held or released, or after `timeout` seconds."""
        if self._held_changed is None:
            self._held_changed = asyncio.Event()
        event = self._held_changed
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(event.wait(), timeout)

    def _hold(self, message: dict[str, Any], ws):
        params = message.get("params")
        params = params if isinstance(params, dict) else {}
        request_id = "rq-" + uuid.uuid4().hex[:12]
        now = time.time()
        entry = {
            "requestId": request_id,
            "type": HELD_METHODS[message["method"]],
            "method": message["method"],
            "threadId": params.get("threadId"),
            "turnId": params.get("turnId"),
            "itemId": params.get("itemId"),
            "createdAt": round(now, 3),
            "deadlineAt": round(now + self.request_timeout, 3),
            "params": params,
            "_rpc_id": message["id"],
            "_message": message,
            "_ws": ws,
        }
        entry["_timer"] = asyncio.get_running_loop().call_later(
            self.request_timeout, lambda: asyncio.ensure_future(self._expire(request_id))
        )
        self.held[request_id] = entry
        self._notify_held()

    def _release(self, request_id: str):
        entry = self.held.pop(request_id, None)
        if entry is not None:
            entry["_timer"].cancel()
            self._notify_held()
        return entry

    async def _expire(self, request_id: str):
        entry = self._release(request_id)
        if entry is None:
            return
        self._record_decline(
            entry["_message"], f"timed out after {self.request_timeout:g}s; auto-declined"
        )
        with contextlib.suppress(Exception):
            await entry["_ws"].send(
                json.dumps({"id": entry["_rpc_id"], "result": DECLINE_RESULTS[entry["method"]]})
            )

    def _drop_held(self, reason: str, thread_id=None, turn_id=None, rpc_id=None, ws=None):
        """Forget held requests the App Server no longer waits on; keep a record of them."""
        for request_id, entry in list(self.held.items()):
            if thread_id is not None and entry["threadId"] != thread_id:
                continue
            if turn_id is not None and entry["turnId"] not in (None, turn_id):
                continue
            if rpc_id is not None and entry["_rpc_id"] != rpc_id:
                continue
            if ws is not None and entry["_ws"] is not ws:
                continue
            self._release(request_id)
            self._record_decline(entry["_message"], reason)

    def pending_for(self, thread_id: str | None = None, turn_id: str | None = None):
        """Public view of held requests, oldest first, optionally for one thread / turn."""
        now = time.time()
        result = []
        for entry in sorted(self.held.values(), key=lambda e: e["createdAt"]):
            if thread_id is not None and entry["threadId"] != thread_id:
                continue
            if turn_id is not None and entry["turnId"] not in (None, turn_id):
                continue
            view = {k: v for k, v in entry.items() if not k.startswith("_") and k != "params"}
            view["secondsRemaining"] = max(0, round(entry["deadlineAt"] - now, 1))
            view["request"] = {
                k: v for k, v in entry["params"].items() if k not in {"threadId", "turnId"}
            }
            result.append(view)
        return result

    async def answer(self, request_id: str, result: dict[str, Any]):
        """Send `result` for a held request. The caller has already validated it."""
        entry = self._release(request_id)
        if entry is None:
            raise KeyError(request_id)
        try:
            await entry["_ws"].send(json.dumps({"id": entry["_rpc_id"], "result": result}))
        except Exception as error:
            raise TransportError(
                "App Server connection lost before the answer was sent; "
                "the request is gone (the turn is handled by the App Server)"
            ) from error

    def _record_decline(self, message: dict[str, Any], answer: str):
        params = message.get("params")
        params = params if isinstance(params, dict) else {}
        questions = params.get("questions")
        first_question = (
            questions[0].get("question")
            if isinstance(questions, list) and questions and isinstance(questions[0], dict)
            else None
        )
        detail = (
            params.get("command")
            or params.get("reason")
            or first_question
            or params.get("message")
            or params.get("serverName")
            or ""
        )
        method = message.get("method")
        record = {
            "method": method,
            "category": (
                "question"
                if method in HELD_METHODS
                else "unsupported"
                if method not in DECLINE_RESULTS
                else "approval"
            ),
            "threadId": params.get("threadId") or params.get("conversationId"),
            "turnId": params.get("turnId"),
            "detail": str(detail)[:300],
            "answer": answer,
            "at": round(time.time(), 3),
        }
        self.declined.append(record)
        if self.declined_log is not None:
            try:
                self.declined_log.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                fd = os.open(self.declined_log, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o600)
                with os.fdopen(fd, "a", encoding="utf-8") as handle:
                    handle.write(json.dumps(record) + "\n")
            except OSError:
                pass  # in-memory record remains

    def declined_for(self, thread_id: str, turn_id: str | None = None):
        """Recorded declines for a thread (and turn, when the request named one)."""
        records = list(self.declined)
        if self.declined_log is not None:
            try:
                with open(self.declined_log, "rb") as handle:
                    handle.seek(0, os.SEEK_END)
                    start = max(0, handle.tell() - DECLINED_LOG_TAIL_BYTES)
                    handle.seek(start)
                    lines = handle.read().decode("utf-8", "replace").splitlines()
                    lines = lines[1:] if start else lines  # first line may be cut mid-record
                records = [json.loads(line) for line in lines if line.strip()] or records
            except (OSError, ValueError):
                pass
        return [
            r
            for r in records
            if r.get("threadId") == thread_id
            and (turn_id is None or r.get("turnId") in (None, turn_id))
        ]

    async def _answer_server_request(self, ws, message: dict[str, Any]):
        method = message.get("method")
        if method in HELD_METHODS and self.request_timeout > 0:
            self._hold(message, ws)
            return
        if method in DECLINE_RESULTS:
            reply = {"id": message["id"], "result": DECLINE_RESULTS[method]}
            self._record_decline(message, "declined")
        else:
            reply = {
                "id": message["id"],
                "error": {"code": -32601, "message": f"bridge does not handle {method}"},
            }
            self._record_decline(message, "unsupported (-32601)")
        await ws.send(json.dumps(reply))

    async def connect(self):
        async with self._connect_lock:
            if self._reader is not None and not self._reader.done():
                return
            await self.close()
            try:
                if not self.socket_path.exists():
                    raise FileNotFoundError(
                        f"Codex control socket not found: {self.socket_path}. "
                        "Use an existing daemon socket (--socket), or start the managed "
                        "daemon with 'codex app-server daemon start'. The bridge does not "
                        "start or replace app-servers."
                    )
                if self.transport == "proxy" or (
                    self.transport == "auto" and sys.platform == "win32"
                ):
                    self._ws = await ProxyWebSocket.open(
                        self.socket_path, self.timeout, self.codex_binary
                    )
                else:
                    self._ws = await unix_connect(
                        str(self.socket_path),
                        uri="ws://localhost/",
                        open_timeout=self.timeout,
                        close_timeout=2,
                        max_size=16 * 1024 * 1024,
                        # Codex 0.153.4 closes Unix handshakes offering permessage-deflate.
                        compression=None,
                    )
                self._reader = asyncio.create_task(self._receive())
                self.info = await self._request(
                    "initialize",
                    {
                        "clientInfo": {"name": "codex_thread_bridge", "version": __version__},
                        "capabilities": {"experimentalApi": True},
                    },
                )
                await self._ws.send(json.dumps({"method": "initialized", "params": {}}))
            except BaseException:
                await self.close()
                raise

    async def _receive(self):
        failure = "App Server disconnected"
        ws = self._ws
        assert ws is not None
        try:
            async for raw in ws:
                message = json.loads(raw)
                if "method" in message:
                    if self.decline_server_requests and "id" not in message:
                        self._observe_notification(message)
                    if self.decline_server_requests and "id" in message:
                        await self._answer_server_request(ws, message)
                        continue
                    # App Server shares server-request callbacks across subscribers;
                    # the first reply (even an error) consumes the callback. Leave
                    # these requests for a capable client subscribed to the thread.
                    # Silence does not guarantee another client is present.
                    # Reads and waits query the server; no unbounded event history.
                    continue
                future = self._pending.get(message.get("id"))
                if future is not None and not future.done():
                    future.set_result(message)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            failure = f"App Server transport failed: {type(error).__name__}: {error}"
        finally:
            self._drop_held("lost: App Server connection ended; request is gone", ws=ws)
            for future in list(self._pending.values()):
                if not future.done():
                    future.set_exception(TransportError(failure))

    def _observe_notification(self, message: dict[str, Any]):
        """Clean up held requests when the App Server resolves them or their turn ends."""
        method, params = message.get("method"), message.get("params")
        params = params if isinstance(params, dict) else {}
        if method == "serverRequest/resolved":
            self._drop_held(
                "resolved by the App Server before an answer",
                params.get("threadId"),
                rpc_id=params.get("requestId"),
            )
        elif method == "turn/completed":
            turn = params.get("turn") if isinstance(params.get("turn"), dict) else {}
            if params.get("threadId") and turn.get("id"):
                self._drop_held(
                    "turn ended before an answer", params["threadId"], turn_id=turn["id"]
                )

    async def _request(
        self, method: str, params: dict[str, Any] | None, timeout: float | None = None
    ):
        ws = self._ws
        if ws is None:
            raise TransportError("App Server is not connected")
        self._counter += 1
        ident = self._counter
        future = asyncio.get_running_loop().create_future()
        self._pending[ident] = future
        try:
            await ws.send(json.dumps({"id": ident, "method": method, "params": params}))
            message = await asyncio.wait_for(future, timeout or self.timeout)
        except (OSError, TimeoutError) as error:
            raise TransportError(f"{method}: response unavailable; do not resend") from error
        finally:
            self._pending.pop(ident, None)
            if not future.done():
                future.cancel()
        if "error" in message:
            raise RpcError(method, message["error"])
        if "result" not in message:
            raise TransportError(f"{method}: invalid response; outcome unknown")
        return message["result"]

    async def call(self, method: str, params: dict[str, Any] | None, timeout: float | None = None):
        await self.connect()
        # Reconnect before a new request, never retry an already-sent mutation.
        try:
            return await self._request(method, params, timeout)
        except TransportError:
            # Idle connections may be closed by a relay (e.g. after ~300s). Pure reads are safe
            # to repeat once on a fresh connection; mutations are never resent.
            if not self.retry_reads or method not in READ_ONLY_METHODS:
                raise
        await self.close()
        await self.connect()
        return await self._request(method, params, timeout)

    async def close(self):
        if self._ws is not None:
            await self._ws.close()
            self._ws = None
        if self._reader is not None:
            self._reader.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reader
            self._reader = None
