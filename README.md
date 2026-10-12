# codex-thread-bridge

An MCP server for creating, messaging, and steering Codex tasks through a running
App Server on the same host.

## Install

Requires Python 3.11+, [uv](https://docs.astral.sh/uv/), and an authenticated Codex
App Server with a Unix WebSocket control socket. Git is required for worktree
launches. The bridge runs as the same user and on the same host as App Server.

From a clone of this repository:

```sh
uv sync --frozen --no-dev
.venv/bin/codex-thread-bridge --help
codex mcp add codex-thread-bridge -- /absolute/path/codex-thread-bridge/.venv/bin/codex-thread-bridge
```

The default socket is `$CODEX_HOME/app-server-control/app-server-control.sock`;
`CODEX_HOME` defaults to `~/.codex`. Use `--socket /path/to.sock` to override it.
Authentication and model usage belong to the existing App Server.

On Windows, the bridge uses `codex app-server proxy --sock` as a raw WebSocket
byte relay. On Unix, it connects directly to the socket. Use `--transport proxy`
to select the relay explicitly, and `--codex-binary /path/to/codex` if the CLI
is not on `PATH`. The equivalent environment variables are
`CODEX_THREAD_BRIDGE_TRANSPORT` and `CODEX_THREAD_BRIDGE_CODEX`.

After changing bridge code or MCP configuration, request a refresh with:

```sh
uv run --locked codex-thread-bridge-reload
```

The command accepts `--socket`, asks for `y/N` confirmation, and queues a refresh
for loaded tasks. Its response does not confirm that every task has refreshed.

## Tools

| Tool | Behavior |
| --- | --- |
| `get_capabilities` | Report server identity and bridge capabilities |
| `create_thread` | Create a retained task in an existing directory, with an optional title and initial prompt |
| `create_worktree_thread` | Create a retained, locked Git worktree and a task at a specified commit |
| `update_thread_permissions` | Apply and verify permissions for an idle task with an expected identity |
| `send_message_to_thread` | Resume an idle task and start a turn without settings overrides |
| `steer_thread` | Append a message to the active turn identified by `expected_turn_id` |
| `list_threads` | List unarchived backend tasks without loading them |
| `read_thread` | Read task metadata and paginated history without resuming |
| `wait_thread` | Wait up to 50 seconds for a specified recent turn |
| `get_goal` | Read persistent Goal state |
| `get_operation` | Read the retained receipt for a mutation request |

Tool schemas are exposed through MCP. Their definitions are in
[server.py](src/codex_thread_bridge/server.py).

## Example

Call `create_thread` with these MCP arguments:

```json
{
  "request_id": "repository-overview-001",
  "cwd": "/absolute/path/to/project",
  "title": "Repository overview",
  "prompt": "Summarize the project structure."
}
```

Pass the returned `threadId` and `turnId` to `wait_thread`. Use
`send_message_to_thread` for an idle task or `steer_thread` for an active turn.
Each intentional new message requires a new `request_id`.

## Behavior

- `create_thread` defaults to sandbox `read-only` and approval policy `never`.
  Explicit `on-request` approval requires App Server Auto-review. Omitted model
  and reasoning effort use server defaults. Creation rejects explicit network-enabled
  read-only policies; permission updates accept them.
- Permission updates require the task's expected identity and an idle task.
  The idle check and update are separate operations, so concurrent clients can
  race them. Workspace writable roots cannot be existing files, sockets, or devices.
- Each mutation uses a stable `request_id`. Repeating it with matching arguments
  returns the recorded receipt without repeating or continuing the operation.
  `accepted` reports API acceptance; turn completion is reported by `wait_thread`.
  Failed or uncertain operations can leave tasks, turns, or worktrees behind;
  `get_operation` returns the recorded outcome and known IDs.
- Receipts persist in `$XDG_STATE_HOME/codex-thread-bridge`, defaulting to
  `~/.local/state/codex-thread-bridge`. `--state-dir` overrides this location.
  Deleting this state discards request deduplication history.
- Worktree launches require a full local commit ID and an absent, canonical
  absolute destination outside existing repositories. Worktrees are detached,
  locked, and retained until manual cleanup. Dirty files are not copied, and Git
  hooks and checkout filters are disabled. Approval policy is `never`;
  `expected_sandbox_policy` verifies returned settings rather than applying overrides.
- Project IDs belong to App Server's registry. Desktop controls its own project
  association and task listing. Worktrees created by the bridge have a manual
  lifecycle rather than a Desktop-managed lifecycle.
- New and resumed turns carry the prompt as user text `input`. The bridge leaves client-side tool calls and approval requests
  unanswered so it cannot consume another client's shared callback. A capable
  client such as Desktop must be subscribed to the thread to handle them; the
  bridge does not establish that subscription or provide interactive approvals.
  Dispatch keeps the shared connection open so concurrent reads can finish.

## Development

```sh
uv sync --frozen --group dev
uv run pytest
uv run ty check src
uv run ruff check .
uv run ruff format --check .
uv build
```

Tests use a fake App Server and temporary Git repositories. Test temporary
directories must be outside existing repositories; pytest accepts `--basetemp`.

The [source](src/codex_thread_bridge) and [tests](tests) define the detailed
behavior. Contribution requirements are in [CONTRIBUTING.md](CONTRIBUTING.md).

## References

- [Codex App Server API documentation](https://learn.chatgpt.com/docs/app-server)
- [RPC method definitions](https://github.com/openai/codex/blob/main/codex-rs/app-server-protocol/src/protocol/common.rs)
- [Request, response, and notification types](https://github.com/openai/codex/tree/main/codex-rs/app-server-protocol/src/protocol/v2)
- [App Server implementation](https://github.com/openai/codex/tree/main/codex-rs/app-server/src)
- [Shared callback routing in Codex 0.153.4](https://github.com/openai/codex/blob/rust-v0.153.4/codex-rs/app-server/src/outgoing_message.rs)

MIT licensed. Independent project, not affiliated with or endorsed by OpenAI.

## Remote mode (fork patches)

This fork adds `--remote-root /abs/posix/path` for an App Server on another host, typically
reached through an `ssh -L <local.sock>:<remote.sock>` forward (`--socket <local.sock>
--transport unix`). In remote mode:

- no local filesystem checks; `cwd`, `list_threads(cwd)` and `writableRoots` must be clean absolute
  POSIX paths equal to or under the remote root (no `..`, `.`, `//`, trailing `/`);
- only `read-only` / `workspace-write` sandboxes and `approvalPolicy: never`; threads outside the
  root, or resumed with another sandbox, are not driven;
- approval-type server requests (command, file change, permissions, legacy exec/patch approvals)
  are declined at once; unknown ones get JSON-RPC `-32601`; so a turn never waits forever. Each is
  recorded in memory and in `<state-dir>/declined-requests.jsonl` (with a `category`: `approval`,
  `question` or `unsupported`) and listed as `declinedRequests` by `get_turn_result` and
  `read_thread`;
- question-type requests (`item/tool/requestUserInput`, `mcpServer/elicitation/request`) are
  **held**, not answered. They live in the bridge process, shared by all MCP sessions, and are
  listed as `pendingRequests` (`requestId`, `type` `userInput`|`elicitation`, thread/turn, the
  question payload, `deadlineAt`, `secondsRemaining`) by `wait_thread`, `read_thread` and
  `get_turn_result`. A `wait_thread` that is blocked returns as soon as a request becomes pending
  (`waitingForAnswer: true`; it also returns immediately if one is already pending).
  `answer_request(request_id, answers=None, decline=False)` settles one: for `userInput`,
  `answers` maps every question id to a string or list of strings; for `elicitation`, `answers` is
  the form content checked against `requestedSchema` (`{}` accepts a `url` elicitation;
  `openai/userVerification` can only be declined). On `--request-timeout SECONDS` (default 600; 0
  declines at once) the request is auto-declined and recorded. Held requests are dropped (and
  recorded) when the App Server resolves them, their turn completes, or the App Server connection
  drops. They are not persisted: **restarting the bridge loses them** and the App Server then
  handles the turn on its own;
- worktrees are managed on the App Server host through `command/exec` (fixed `git worktree`
  argv, never a shell string, sandbox `workspaceWrite` limited to `<root>/.git` and
  `<root>/.worktrees`, timeout enforced by the server). `create_worktree_thread(request_id, name,
  base_ref, sandbox, branch?, prompt?, title?, model?, reasoning_effort?)` replaces the local form
  and creates `<root>/.worktrees/<name>` (`name` = `^[a-z0-9][a-z0-9._-]{0,63}$`; `base_ref`
  required, no fetch; `branch` makes a new branch without upstream, otherwise `--detach`);
  `remove_worktree(name, force=false)` only removes registered worktrees directly under
  `.worktrees/`; `list_worktrees()` lists them. There is no generic command tool;
- `--worktree-setup PATH` (remote mode, optional) names one executable on the App Server host
  that `create_worktree_thread` runs as `[PATH, <worktree>]` after checkout, in the same sandbox
  and before the task starts (for example, copying untracked agent context into the worktree).
  Its output is in the receipt under `worktree.setup`; a non-zero exit fails the operation and
  withholds the task;
- read-only requests (`thread/read`, `thread/list`, `thread/turns/list`, ...) are retried once on a
  fresh connection if the connection drops (relays may close idle connections), so `wait_thread`
  survives it; mutations are never resent;
- `--endpoint-id NAME` keys the operation ledger by name instead of by socket path.

`get_capabilities` reports the real transports: `mcpTransport` (`stdio` or `streamable-http on
unix socket <path>`) and `appServerConnection` (unix WebSocket control socket, or the codex CLI
proxy).

New tools (all modes): `interrupt_turn(thread_id, turn_id)` and
`get_turn_result(thread_id, turn_id, max_text_chars=20000)` (final agent message, `fileChange`
diffs, command summary, declined requests). Tests: `uv run pytest` (see `tests/test_remote.py`).

## Streamable HTTP on a unix socket (fork patch)

Default MCP transport is still stdio. To run the bridge next to the App Server and reach it through
an ssh socket forward:

```
codex-thread-bridge --mcp-transport streamable-http \
  --mcp-socket /path/dir700/bridge.sock [--remote-root /code ...]
```

- The socket is created mode 600 in an existing, user-owned 700 directory; a symlink or non-socket
  at the path is refused; only a stale socket (no listener) is replaced; a live one is an error.
- There is no application-level authentication: the socket (600, same-UID, in a 700 directory) is the
  access control. Do not expose it beyond a trusted tunnel.
- The endpoint is `/mcp`. The server is stateless: each request is its own MCP session and all
  share one bridge (one App Server connection, one ledger), so concurrent clients see the same
  threads and a client reconnecting after a bridge restart needs no session recovery.
