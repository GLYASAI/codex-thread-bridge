"""Tool behavior, independent of MCP transport and the installed client."""

import asyncio
import contextlib
import posixpath
import uuid
from pathlib import Path, PurePosixPath

from . import remote_worktrees as rw
from .ledger import Ledger
from .rpc import AppServer, RpcError, TransportError
from .worktrees import Worktree, WorktreeError


def nonempty(value: str, name: str, maximum: int = 100_000):
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ValueError(f"{name} must contain 1–{maximum} characters")


def absolute_directory(cwd: str):
    path = Path(cwd)
    if not path.is_absolute() or not path.is_dir():
        raise ValueError("cwd must be an existing absolute directory on the App Server host")
    return str(path.resolve())


# --- charm-codex remote mode (references/upstream-sync.md, patch 1) -----------------------------
REMOTE_SANDBOXES = frozenset({"read-only", "workspace-write"})
REMOTE_SANDBOX_TYPES = frozenset({"readOnly", "workspaceWrite"})
TERMINAL_STATUSES = frozenset({"completed", "failed", "interrupted"})


def remote_directory(path, root: str, name: str = "cwd"):
    """Validate a path on the *remote* App Server host without touching the local filesystem.

    Pure POSIX string handling: absolute, no '..' / '.' / empty segments or control characters,
    and equal to or under `root`. Returns the (already clean) path unchanged.
    """
    if not isinstance(path, str) or not path or any(ord(c) < 0x20 or ord(c) == 0x7F for c in path):
        raise ValueError(f"{name} must be a non-empty path without control characters")
    if not path.startswith("/"):
        raise ValueError(f"{name} must be an absolute POSIX path on the remote host")
    if any(part in {"", ".", ".."} for part in path.split("/")[1:]):
        raise ValueError(f"{name} must not contain '..', '.', empty segments or a trailing slash")
    if path != root and not PurePosixPath(path).is_relative_to(PurePosixPath(root)):
        raise ValueError(f"{name} must be under the configured repository root {root}")
    return path


DISPLAY_FIELDS = frozenset({"text", "preview", "summary", "objective", "aggregatedOutput"})
SANDBOX_POLICY_TYPES = {
    "read-only": "readOnly",
    "workspace-write": "workspaceWrite",
    "danger-full-access": "dangerFullAccess",
}


def validate_sandbox_policy(policy: dict):
    fields = {
        "readOnly": {"type", "networkAccess"},
        "dangerFullAccess": {"type"},
        "workspaceWrite": {
            "type",
            "networkAccess",
            "writableRoots",
            "excludeTmpdirEnvVar",
            "excludeSlashTmp",
        },
    }
    if set(policy) != fields.get(policy.get("type")):
        raise ValueError("Expected sandbox policy must contain all and only its protocol fields")
    for key in ("networkAccess", "excludeTmpdirEnvVar", "excludeSlashTmp"):
        if key in policy and type(policy[key]) is not bool:
            raise ValueError("Expected sandbox policy flags must be booleans")
    if "writableRoots" in policy and (
        not isinstance(policy["writableRoots"], list)
        or any(not isinstance(p, str) or not Path(p).is_absolute() for p in policy["writableRoots"])
    ):
        raise ValueError("Expected sandbox policy writableRoots must be absolute paths")


def validate_workspace_roots(policy):
    for value in policy.get("writableRoots", []):
        path = Path(value)
        if (path.exists() or path.is_symlink()) and not path.is_dir():
            raise ValueError("writableRoots must be directories, not files, sockets or devices")


def validate_execution_policy(approval_policy, approvals_reviewer):
    if approval_policy not in {"never", "on-request"}:
        raise ValueError("Supported policies are never or on-request with App Server Auto-review")
    if approvals_reviewer not in {"user", "auto_review"}:
        raise ValueError("Unsupported approvals reviewer")
    if approval_policy != "never" and approvals_reviewer != "auto_review":
        raise ValueError("Interactive client approvals unsupported; use App Server Auto-review")


def identity(settings):
    return {
        "thread_id": settings["thread"]["id"],
        "cwd": settings["cwd"],
        "model": settings["model"],
        "reasoning_effort": settings["reasoningEffort"],
    }


def clipped(value, limit: int, *, display_text: bool = False):
    """Bound display content while preserving opaque protocol fields verbatim."""
    if display_text and isinstance(value, str) and len(value) > limit:
        return value[:limit] + f"\n[truncated; original length {len(value)} characters]"
    if isinstance(value, list):
        return [clipped(item, limit, display_text=display_text) for item in value]
    if isinstance(value, dict):
        return {
            key: clipped(item, limit, display_text=key in DISPLAY_FIELDS)
            for key, item in value.items()
        }
    return value


class Bridge:
    def __init__(
        self,
        rpc: AppServer,
        ledger: Ledger,
        remote_root: str | None = None,
        mcp_transport: str = "stdio",
        worktree_setup: str | None = None,
    ):
        self.rpc = rpc
        self.ledger = ledger
        # How MCP clients reach this bridge, e.g. "stdio" or "streamable-http on unix socket <path>".
        self.mcp_transport = mcp_transport
        self._mutation_lock = asyncio.Lock()
        # Remote mode: the App Server runs on another host (reached through an ssh socket
        # forward). Local filesystem checks are skipped; paths must lie under remote_root.
        if remote_root is not None and (
            not remote_root.startswith("/")
            or remote_root == "/"
            or posixpath.normpath(remote_root) != remote_root
        ):
            raise ValueError("remote_root must be a clean absolute POSIX path other than /")
        self.remote_root = remote_root
        # Remote mode: an executable on the App Server host run as [worktree_setup, <worktree>]
        # after each new worktree is checked out and before its task starts.
        if worktree_setup is not None and (
            remote_root is None
            or not worktree_setup.startswith("/")
            or posixpath.normpath(worktree_setup) != worktree_setup
        ):
            raise ValueError("worktree_setup requires remote mode and a clean absolute POSIX path")
        self.worktree_setup = worktree_setup

    @property
    def remote(self) -> bool:
        return self.remote_root is not None

    def _directory(self, cwd, name="cwd"):
        if self.remote:
            return remote_directory(cwd, self.remote_root, name)
        return absolute_directory(cwd)

    def _check_roots(self, policy):
        if self.remote:
            for value in policy.get("writableRoots", []):
                remote_directory(value, self.remote_root, "writableRoots entry")
        else:
            validate_workspace_roots(policy)

    def _check_remote_policy(self, sandbox=None, sandbox_policy=None, approval_policy="never"):
        if not self.remote:
            return
        if sandbox is not None and sandbox not in REMOTE_SANDBOXES:
            raise ValueError("Remote mode allows only sandbox read-only or workspace-write")
        if sandbox_policy is not None and sandbox_policy.get("type") not in REMOTE_SANDBOX_TYPES:
            raise ValueError("Remote mode allows only readOnly or workspaceWrite sandbox policies")
        if approval_policy != "never":
            raise ValueError("Remote mode requires approval_policy never")

    async def _remote_thread(self, thread_id):
        """Remote mode: only drive threads whose cwd lies under the repository root."""
        if not self.remote:
            return
        state = (await self.rpc.call("thread/read", {"threadId": thread_id}))["thread"]
        try:
            remote_directory(state.get("cwd"), self.remote_root, "thread cwd")
        except ValueError as error:
            raise ValueError(f"Refusing to drive this thread: {error}") from None

    def _check_remote_sandbox(self, settings):
        if self.remote and (settings.get("sandbox") or {}).get("type") not in REMOTE_SANDBOX_TYPES:
            raise RpcError(
                "thread/resume",
                {
                    "code": "remote_sandbox_not_allowed",
                    "message": "Remote mode drives only readOnly/workspaceWrite threads; "
                    "message withheld",
                },
            )

    async def capabilities(self):
        await self.rpc.connect()
        return {
            "server": self.rpc.info,
            "transport": f"MCP over {self.mcp_transport}; App Server via "
            f"{self.rpc.connection} {self.rpc.socket_path}",
            "mcpTransport": self.mcp_transport,
            "appServerConnection": {
                "kind": self.rpc.connection,
                "socket": str(self.rpc.socket_path),
            },
            "socket": str(self.rpc.socket_path),
            "capabilities": {
                "createThread": True,
                "sendMessage": True,
                "steerActiveTurn": True,
                "listReadWait": True,
                "goalRead": True,
                "goalSet": False,
                "desktopManagedWorktrees": False,
                "bridgeManagedWorktrees": not self.remote,
                "desktopProjectRegistry": False,
                "clientSideToolsAndApprovals": (
                    "approvals declined and recorded; questions held for answer_request"
                    if self.remote
                    else False
                ),
                "answerRequest": self.remote,
                "interruptTurn": True,
                "turnResult": True,
            },
            "remoteRoot": self.remote_root,
            "pendingRequests": {
                "held": len(self.rpc.held),
                "timeoutSeconds": self.rpc.request_timeout,
                "note": "Held requests live in this process only; a bridge restart loses them.",
            },
            "desktopVisibility": "Observed on Codex 0.153.4 with an existing project checkout; "
            "verify actual Desktop listing for each launch. Backend project IDs are separate.",
        }

    async def _mutate(
        self, request_id, method, params, action, *, validate_fresh=None, legacy_params=None
    ):
        async with self._mutation_lock:
            retained = self.ledger.lookup(request_id, method, params, legacy_params=legacy_params)
            if retained is not None:
                return {**retained, "replayed": True}
            if validate_fresh is not None:
                validate_fresh()
            fresh, receipt = self.ledger.begin(
                request_id, method, params, legacy_params=legacy_params
            )
            if not fresh:
                return {**receipt, "replayed": True}
            try:
                await action(receipt)
                receipt["status"] = "accepted"
            except RpcError as error:
                receipt.update(status="failed", error=str(error), rpcError=error.error)
            except WorktreeError as error:
                receipt.update(status="failed", error=str(error))
            except asyncio.CancelledError:
                self.ledger.save({**receipt, "status": "outcome_unknown"})
                raise
            except Exception as error:
                receipt.update(status="outcome_unknown", error=f"{type(error).__name__}: {error}")
            return self.ledger.save(receipt)

    async def create_thread(
        self,
        request_id: str,
        cwd: str,
        prompt: str | None = None,
        title: str | None = None,
        sandbox: str = "read-only",
        model: str | None = None,
        app_server_project_id: str | None = None,
        sandbox_policy: dict | None = None,
        approval_policy: str = "never",
        approvals_reviewer: str = "auto_review",
        reasoning_effort: str | None = None,
    ):
        validate_execution_policy(approval_policy, approvals_reviewer)
        self._check_remote_policy(sandbox, sandbox_policy, approval_policy)
        if reasoning_effort is not None:
            nonempty(reasoning_effort, "reasoning_effort", 128)
        if sandbox_policy is not None:
            validate_sandbox_policy(sandbox_policy)
            if sandbox_policy["type"] != SANDBOX_POLICY_TYPES.get(sandbox):
                raise ValueError("sandbox and sandbox_policy.type must agree")
        nonempty(cwd, "cwd")
        if self.remote:
            cwd = self._directory(cwd)
        if not Path(cwd).is_absolute():
            raise ValueError("cwd must be an existing absolute directory on the App Server host")
        if sandbox not in SANDBOX_POLICY_TYPES:
            raise ValueError("Unsupported sandbox")
        for name, value in [("prompt", prompt), ("title", title), ("model", model)]:
            if value is not None:
                nonempty(value, name, 100_000 if name == "prompt" else 500)
        params = {"cwd": cwd, "sandbox": sandbox, "approvalPolicy": "never", "ephemeral": False}
        if approval_policy != "never":
            params.update(approvalPolicy=approval_policy, approvalsReviewer=approvals_reviewer)
        if model is not None:
            params["model"] = model
        if app_server_project_id is not None:
            nonempty(app_server_project_id, "app_server_project_id", 128)
            params["projectId"] = app_server_project_id

        config_overrides: dict[str, object] = {}
        if sandbox_policy is not None:
            params["approvalsReviewer"] = approvals_reviewer
            if sandbox_policy["type"] == "workspaceWrite":
                config_overrides = {
                    "sandbox_workspace_write": {
                        "writable_roots": sandbox_policy["writableRoots"],
                        "network_access": sandbox_policy["networkAccess"],
                        "exclude_tmpdir_env_var": sandbox_policy["excludeTmpdirEnvVar"],
                        "exclude_slash_tmp": sandbox_policy["excludeSlashTmp"],
                    }
                }
            elif sandbox_policy["type"] == "readOnly" and sandbox_policy["networkAccess"]:
                raise ValueError(
                    "Network-enabled read-only creation unsupported; use a workspace policy"
                )
        if reasoning_effort is not None:
            config_overrides["model_reasoning_effort"] = reasoning_effort
        if config_overrides:
            params["config"] = config_overrides
        launch_params = dict(params)
        request_params = {**params, "prompt": prompt, "title": title}
        if sandbox_policy is not None:
            request_params["sandbox_policy"] = sandbox_policy
        request_params["approvals_reviewer"] = approvals_reviewer

        def legacy_params(version):
            previous = dict(request_params)
            if sandbox_policy is None:
                previous.pop("approvals_reviewer")
            yield previous
            # Try the original spelling before resolving a possibly changed path.
            # Only version 1 also accepted resolved cwd; version 2 used the supplied path.
            if version == 1:
                yield {**previous, "cwd": str(Path(cwd).resolve())}

        def validate_fresh():
            # The fingerprint uses the supplied path, not mutable symlink resolution.
            launch_params["cwd"] = self._directory(cwd)
            if sandbox_policy is not None:
                self._check_roots(sandbox_policy)

        async def action(receipt):
            if app_server_project_id is not None:
                await self.rpc.call("project/read", {"projectId": app_server_project_id})
            created = await self.rpc.call("thread/start", launch_params)
            thread_id = created["thread"]["id"]
            receipt.update(threadId=thread_id, creation=created)
            self.ledger.save(receipt)  # Retain the ID even if naming or the first turn fails.
            actual = created.get("sandbox", {}).get("type")
            expected = SANDBOX_POLICY_TYPES[sandbox]
            if (
                created.get("cwd") != launch_params["cwd"]
                or created.get("approvalPolicy") != approval_policy
                or actual != expected
                or (model is not None and created.get("model") != model)
                or (
                    reasoning_effort is not None
                    and created.get("reasoningEffort") != reasoning_effort
                )
            ):
                raise RpcError(
                    "thread/start",
                    {
                        "code": "environment_mismatch",
                        "message": "Created environment differs; initial prompt withheld. "
                        "Inspect creation receipt. The thread remains retained.",
                    },
                )
            if (sandbox_policy is not None and created.get("sandbox") != sandbox_policy) or (
                (sandbox_policy is not None or approval_policy != "never")
                and created.get("approvalsReviewer") != approvals_reviewer
            ):
                raise RpcError(
                    "thread/start",
                    {
                        "code": "environment_mismatch",
                        "message": "Effective permission policy differs; prompt withheld",
                    },
                )
            if sandbox_policy is not None:
                receipt["permissionsAfter"] = created
                receipt["permissionUpdateState"] = "verified"
                self.ledger.save(receipt)
            if title is not None:
                await self.rpc.call("thread/name/set", {"threadId": thread_id, "name": title})
                receipt["title"] = title
                self.ledger.save(receipt)
            if prompt is not None:
                turn = await self.rpc.call(
                    "turn/start",
                    {
                        "threadId": thread_id,
                        "input": [{"type": "text", "text": prompt}],
                    },
                )
                receipt["turnId"] = turn["turn"]["id"]
            receipt["desktopProjectAssociation"] = "unverified; check Desktop listing"

        return await self._mutate(
            request_id,
            "create_thread",
            request_params,
            action,
            validate_fresh=validate_fresh,
            legacy_params=legacy_params,
        )

    async def create_worktree_thread(
        self,
        request_id: str,
        source_repository: str,
        starting_revision: str,
        destination: str,
        worktree_mode: str,
        sandbox: str,
        expected_sandbox_policy: dict,
        prompt: str | None = None,
        title: str | None = None,
        model: str | None = None,
        reasoning_effort: str | None = None,
        app_server_project_id: str | None = None,
    ):
        if self.remote:
            raise ValueError(
                "The local create_worktree_thread form is unavailable in remote mode; use "
                "create_remote_worktree_thread (MCP: create_worktree_thread with name/base_ref)."
            )
        if worktree_mode != "bridge-managed-retained":
            raise ValueError("Explicit bridge-managed-retained worktree ownership is required")
        validate_sandbox_policy(expected_sandbox_policy)
        if (
            sandbox not in SANDBOX_POLICY_TYPES
            or expected_sandbox_policy.get("type") != SANDBOX_POLICY_TYPES[sandbox]
        ):
            raise ValueError("sandbox and expected_sandbox_policy.type must agree")
        for name, value in [
            ("source_repository", source_repository),
            ("starting_revision", starting_revision),
            ("destination", destination),
            ("prompt", prompt),
            ("title", title),
            ("model", model),
            ("reasoning_effort", reasoning_effort),
            ("app_server_project_id", app_server_project_id),
        ]:
            if value is not None:
                nonempty(value, name)
        params = {
            "source_repository": source_repository,
            "starting_revision": starting_revision,
            "destination": destination,
            "worktree_mode": worktree_mode,
            "sandbox": sandbox,
            "expected_sandbox_policy": expected_sandbox_policy,
            "prompt": prompt,
            "title": title,
            "model": model,
            "reasoning_effort": reasoning_effort,
            "app_server_project_id": app_server_project_id,
        }

        async def action(receipt):
            def checkpoint(phase, **fields):
                receipt.update(phase=phase, **fields)
                self.ledger.save(receipt)

            checkpoint(
                "validating",
                recoveryRequired=True,
                recovery="Inspect this receipt, the destination and Git worktree list, and "
                "backend/Desktop tasks before manual recovery. Retain all artifacts; do not "
                "retry with a new request ID. Unknown thread/turn outcomes need reconciliation.",
                requestedCheckout=destination,
                initialPrompt={"state": "not_sent" if prompt is not None else "not_requested"},
                desktopProjectAssociation={
                    "status": "unverified",
                    "sourceRepository": source_repository,
                },
            )
            worktree = await Worktree.validate(source_repository, starting_revision, destination)
            if app_server_project_id is not None:
                await self.rpc.call("project/read", {"projectId": app_server_project_id})
            checkpoint("reserving_destination", worktree=worktree.receipt())
            worktree.reserve()
            receipt["worktree"]["state"] = "reserved"
            checkpoint("creating_worktree")
            await worktree.create()
            receipt["worktree"]["state"] = "registered"
            checkpoint("checking_out_worktree")
            await worktree.checkout()
            receipt["worktree"]["state"] = "created"
            checkpoint("checking_worktree")
            actual = await worktree.inspect()
            receipt["worktree"].update(actual)
            checkpoint("worktree_checked")
            if not worktree.matches(actual):
                raise WorktreeError("Worktree placement/base mismatch; initial prompt withheld")

            launch = {
                "cwd": str(worktree.destination),
                "sandbox": sandbox,
                "approvalPolicy": "never",
                "ephemeral": False,
                "runtimeWorkspaceRoots": [str(worktree.destination)],
            }
            if model is not None:
                launch["model"] = model
            if reasoning_effort is not None:
                launch["config"] = {"model_reasoning_effort": reasoning_effort}
            if app_server_project_id is not None:
                launch["projectId"] = app_server_project_id
            checkpoint("creating_thread")
            created = await self.rpc.call("thread/start", launch)
            checkpoint(
                "checking_environment",
                threadId=created["thread"]["id"],
                creation=created,
                permissionReceipt={
                    key: created.get(key)
                    for key in (
                        "approvalPolicy",
                        "sandbox",
                        "activePermissionProfile",
                        "runtimeWorkspaceRoots",
                    )
                },
                desktopProjectAssociation={
                    "status": "unverified",
                    "sourceRepository": source_repository,
                    "checkout": created.get("cwd"),
                    "appServerProjectId": created["thread"].get("projectId"),
                },
            )
            if (
                created.get("cwd") != str(worktree.destination)
                or created["thread"].get("cwd") != str(worktree.destination)
                or created.get("runtimeWorkspaceRoots") != [str(worktree.destination)]
                or created.get("approvalPolicy") != "never"
                or created.get("sandbox") != expected_sandbox_policy
                or (model is not None and created.get("model") != model)
                or (
                    reasoning_effort is not None
                    and created.get("reasoningEffort") != reasoning_effort
                )
                or (
                    app_server_project_id is not None
                    and created["thread"].get("projectId") != app_server_project_id
                )
            ):
                raise WorktreeError(
                    "Created environment differs; initial prompt withheld. Inspect creation receipt"
                )
            if title is not None:
                checkpoint("naming_thread")
                await self.rpc.call(
                    "thread/name/set", {"threadId": receipt["threadId"], "name": title}
                )
                checkpoint("thread_named", title=title)
            # Thread startup and naming can take time; recheck placement just before dispatch.
            actual = await worktree.inspect()
            checkpoint("checking_before_dispatch", checkoutBeforeDispatch=actual)
            if not worktree.matches(actual):
                raise WorktreeError(
                    "Worktree changed during thread startup; initial prompt withheld"
                )
            if prompt is not None:
                checkpoint("dispatching_initial_prompt", initialPrompt={"state": "outcome_unknown"})
                turn = await self.rpc.call(
                    "turn/start",
                    {
                        "threadId": receipt["threadId"],
                        "input": [{"type": "text", "text": prompt}],
                    },
                )
                checkpoint(
                    "initial_prompt_accepted",
                    turnId=turn["turn"]["id"],
                    initialPrompt={"state": "accepted"},
                )
            checkpoint("complete", recoveryRequired=False)

        return await self._mutate(request_id, "create_worktree_thread", params, action)

    # --- remote worktrees through the App Server's command/exec ------------------------------
    async def _git_exec(self, argv, sandbox_policy, timeout_s=60):
        """Run one fixed git argv on the App Server host. Returns the exec result dict."""
        process_id = f"ctb-{uuid.uuid4().hex}"
        params = {
            "command": argv,
            "cwd": self.remote_root,
            "env": rw.GIT_ENV,
            "sandboxPolicy": sandbox_policy,
            "timeoutMs": int(timeout_s * 1000),
            "processId": process_id,
        }
        try:
            result = await self.rpc.call("command/exec", params, timeout=timeout_s + 15)
        except TransportError:
            with contextlib.suppress(Exception):
                await self.rpc.call("command/exec/terminate", {"processId": process_id})
            raise
        return {
            "argv": argv,
            "exitCode": result.get("exitCode"),
            "stdout": clipped(result.get("stdout", ""), 8000, display_text=True),
            "stderr": clipped(result.get("stderr", ""), 8000, display_text=True),
        }

    def _require_remote(self, tool):
        if not self.remote:
            raise ValueError(f"{tool} is available only in remote mode (--remote-root)")

    async def _registered_worktrees(self):
        listing = await self._git_exec(rw.list_argv(self.remote_root), rw.READ_SANDBOX, 30)
        if listing["exitCode"] != 0:
            return listing, None
        return listing, rw.parse_porcelain(listing["stdout"], self.remote_root)

    async def list_worktrees(self):
        self._require_remote("list_worktrees")
        listing, entries = await self._registered_worktrees()
        if entries is None:
            return {"status": "failed", "command": listing}
        return {"status": "ok", "repoRoot": self.remote_root, "worktrees": entries}

    async def remove_worktree(self, name: str, force: bool = False):
        self._require_remote("remove_worktree")
        path = rw.worktree_path(self.remote_root, name)
        if not isinstance(force, bool):
            raise ValueError("force must be a boolean")
        async with self._mutation_lock:
            listing, entries = await self._registered_worktrees()
            if entries is None:
                return {"status": "failed", "command": listing}
            if not any(entry["path"] == path for entry in entries):
                raise ValueError(f"{path} is not a registered worktree of {self.remote_root}")
            removal = await self._git_exec(
                rw.remove_argv(self.remote_root, path, force), rw.write_sandbox(self.remote_root)
            )
        if removal["exitCode"] != 0:
            return {"status": "failed", "path": path, "command": removal}
        return {"status": "removed", "path": path, "forced": force, "command": removal}

    async def create_remote_worktree_thread(
        self,
        request_id: str,
        name: str,
        base_ref: str,
        sandbox: str,
        branch: str | None = None,
        prompt: str | None = None,
        title: str | None = None,
        model: str | None = None,
        reasoning_effort: str | None = None,
    ):
        self._require_remote("create_worktree_thread")
        path = rw.worktree_path(self.remote_root, name)
        rw.validate_ref(base_ref, "base_ref")
        if branch is not None:
            rw.validate_branch(branch)
        self._check_remote_policy(sandbox=sandbox)
        if sandbox not in REMOTE_SANDBOXES:
            raise ValueError("sandbox must be read-only or workspace-write")
        for label, value in [
            ("prompt", prompt),
            ("title", title),
            ("model", model),
            ("reasoning_effort", reasoning_effort),
        ]:
            if value is not None:
                nonempty(value, label)
        params = {
            "name": name,
            "base_ref": base_ref,
            "branch": branch,
            "sandbox": sandbox,
            "prompt": prompt,
            "title": title,
            "model": model,
            "reasoning_effort": reasoning_effort,
        }

        async def action(receipt):
            def checkpoint(phase, **fields):
                receipt.update(phase=phase, **fields)
                self.ledger.save(receipt)

            checkpoint(
                "creating_worktree",
                recoveryRequired=True,
                recovery="Inspect this receipt, list_worktrees and list_threads before manual "
                "recovery. Retain artifacts; do not retry with a new request ID.",
                worktree={"path": path, "baseRef": base_ref, "branch": branch, "state": "planned"},
            )
            added = await self._git_exec(
                rw.add_argv(self.remote_root, path, base_ref, branch),
                rw.write_sandbox(self.remote_root),
                timeout_s=180,
            )
            receipt["worktree"]["add"] = added
            if added["exitCode"] != 0:
                raise WorktreeError(
                    f"git worktree add failed (exit {added['exitCode']}): "
                    f"{added['stderr'] or added['stdout']}"
                )
            receipt["worktree"]["state"] = "created"
            head = await self._git_exec(rw.head_argv(path), rw.READ_SANDBOX, 30)
            if head["exitCode"] != 0:
                raise WorktreeError(f"new worktree is not usable: {head['stderr']}")
            receipt["worktree"]["head"] = head["stdout"].strip()
            if self.worktree_setup is not None:
                checkpoint("setting_up_worktree")
                setup = await self._git_exec(
                    [self.worktree_setup, path], rw.write_sandbox(self.remote_root), timeout_s=120
                )
                receipt["worktree"]["setup"] = setup
                if setup["exitCode"] != 0:
                    raise WorktreeError(
                        f"worktree setup failed (exit {setup['exitCode']}); task not started: "
                        f"{setup['stderr'] or setup['stdout']}"
                    )
            checkpoint("creating_thread")
            launch = {
                "cwd": path,
                "sandbox": sandbox,
                "approvalPolicy": "never",
                "ephemeral": False,
                "runtimeWorkspaceRoots": [path],
            }
            if model is not None:
                launch["model"] = model
            if reasoning_effort is not None:
                launch["config"] = {"model_reasoning_effort": reasoning_effort}
            created = await self.rpc.call("thread/start", launch)
            checkpoint(
                "checking_environment",
                threadId=created["thread"]["id"],
                creation=created,
                permissionReceipt={
                    key: created.get(key)
                    for key in (
                        "approvalPolicy",
                        "sandbox",
                        "activePermissionProfile",
                        "runtimeWorkspaceRoots",
                    )
                },
            )
            if (
                created.get("cwd") != path
                or created["thread"].get("cwd") != path
                or created.get("approvalPolicy") != "never"
                or (created.get("sandbox") or {}).get("type") != SANDBOX_POLICY_TYPES[sandbox]
                or (model is not None and created.get("model") != model)
                or (
                    reasoning_effort is not None
                    and created.get("reasoningEffort") != reasoning_effort
                )
            ):
                raise WorktreeError(
                    "Created environment differs; initial prompt withheld. Inspect creation receipt"
                )
            if title is not None:
                checkpoint("naming_thread")
                await self.rpc.call(
                    "thread/name/set", {"threadId": receipt["threadId"], "name": title}
                )
                checkpoint("thread_named", title=title)
            if prompt is not None:
                checkpoint("dispatching_initial_prompt", initialPrompt={"state": "outcome_unknown"})
                turn = await self.rpc.call(
                    "turn/start",
                    {
                        "threadId": receipt["threadId"],
                        "input": [{"type": "text", "text": prompt}],
                    },
                )
                checkpoint(
                    "initial_prompt_accepted",
                    turnId=turn["turn"]["id"],
                    initialPrompt={"state": "accepted"},
                )
            checkpoint("complete", recoveryRequired=False)

        return await self._mutate(request_id, "create_worktree_thread", params, action)

    async def _idle_settings(self, thread_id):
        state = (await self.rpc.call("thread/read", {"threadId": thread_id}))["thread"]
        if state.get("status", {}).get("type") not in {"idle", "notLoaded"}:
            raise RpcError(
                "thread/read",
                {"code": "thread_busy", "message": "Task is not idle; no settings changed"},
            )
        settings = await self.rpc.call(
            "thread/resume", {"threadId": thread_id, "excludeTurns": True}
        )
        if settings["thread"].get("status", {}).get("type") != "idle":
            raise RpcError(
                "thread/resume",
                {"code": "thread_busy", "message": "Task became active; no settings changed"},
            )
        return settings

    async def _apply_permissions(
        self,
        receipt,
        thread_id,
        sandbox_policy,
        approval_policy,
        approvals_reviewer,
        expected_identity,
    ):
        before = await self._idle_settings(thread_id)
        receipt["permissionsBefore"] = before
        self.ledger.save(receipt)
        if identity(before) != expected_identity:
            raise RpcError(
                "thread/resume",
                {
                    "code": "identity_mismatch",
                    "message": "Task identity/settings differ; no update sent",
                },
            )
        state = (await self.rpc.call("thread/read", {"threadId": thread_id}))["thread"]
        if state.get("status", {}).get("type") != "idle":
            raise RpcError(
                "thread/read",
                {"code": "thread_busy", "message": "Task became active; no update sent"},
            )
        receipt["permissionUpdateState"] = "outcome_unknown"
        self.ledger.save(receipt)
        await self.rpc.call(
            "thread/settings/update",
            {
                "threadId": thread_id,
                "sandboxPolicy": sandbox_policy,
                "approvalPolicy": approval_policy,
                "approvalsReviewer": approvals_reviewer,
            },
        )
        after = await self.rpc.call("thread/resume", {"threadId": thread_id, "excludeTurns": True})
        receipt["permissionsAfter"] = after
        self.ledger.save(receipt)
        if (
            identity(after) != expected_identity
            or after.get("runtimeWorkspaceRoots") != before.get("runtimeWorkspaceRoots")
            or after.get("sandbox") != sandbox_policy
            or after.get("approvalPolicy") != approval_policy
            or after.get("approvalsReviewer") != approvals_reviewer
        ):
            raise RpcError(
                "thread/settings/update",
                {
                    "code": "permission_verification_failed",
                    "message": "Effective settings differ; inspect receipt before further actions",
                },
            )
        receipt["permissionUpdateState"] = "verified"

    async def update_thread_permissions(
        self,
        request_id,
        thread_id,
        sandbox_policy,
        expected_identity,
        approval_policy="never",
        approvals_reviewer="auto_review",
    ):
        nonempty(thread_id, "thread_id", 128)
        validate_sandbox_policy(sandbox_policy)
        validate_execution_policy(approval_policy, approvals_reviewer)
        self._check_remote_policy(None, sandbox_policy, approval_policy)
        if (
            set(expected_identity) != {"thread_id", "cwd", "model", "reasoning_effort"}
            or expected_identity["thread_id"] != thread_id
        ):
            raise ValueError(
                "Expected identity must contain this thread_id, cwd, model, reasoning_effort"
            )
        params = {
            "threadId": thread_id,
            "sandboxPolicy": sandbox_policy,
            "expectedIdentity": expected_identity,
            "approvalPolicy": approval_policy,
            "approvalsReviewer": approvals_reviewer,
        }

        async def action(receipt):
            receipt["threadId"] = thread_id
            await self._remote_thread(thread_id)
            await self._apply_permissions(
                receipt,
                thread_id,
                sandbox_policy,
                approval_policy,
                approvals_reviewer,
                expected_identity,
            )

        return await self._mutate(
            request_id,
            "update_thread_permissions",
            params,
            action,
            validate_fresh=lambda: self._check_roots(sandbox_policy),
        )

    async def send_message_to_thread(self, request_id: str, thread_id: str, message: str):
        nonempty(thread_id, "thread_id", 128)
        nonempty(message, "message")

        async def action(receipt):
            receipt["threadId"] = thread_id
            self.ledger.save(receipt)
            await self._remote_thread(thread_id)
            state = await self.rpc.call("thread/read", {"threadId": thread_id})
            if state["thread"].get("status", {}).get("type") == "active":
                raise RpcError(
                    "thread/read",
                    {
                        "code": "thread_busy",
                        "message": "Thread is active; message withheld. Wait for completion.",
                    },
                )
            # Resume is an explicit part of messaging, never part of discovery.
            # No cwd, model, sandbox, or reasoning overrides are supplied.
            resumed = await self.rpc.call(
                "thread/resume",
                {
                    "threadId": thread_id,
                    "excludeTurns": True,
                },
            )
            receipt["resumed"] = resumed
            self.ledger.save(receipt)
            self._check_remote_sandbox(resumed)
            if resumed.get("approvalPolicy") != "never" and not (
                resumed.get("approvalPolicy") == "on-request"
                and resumed.get("approvalsReviewer") == "auto_review"
            ):
                raise RpcError(
                    "thread/resume",
                    {
                        "code": "unsupported_approval_policy",
                        "message": "Client-side approvals unsupported; message withheld. "
                        "Continue the thread in Desktop.",
                    },
                )
            turn = await self.rpc.call(
                "turn/start",
                {
                    "threadId": thread_id,
                    "input": [{"type": "text", "text": message}],
                },
            )
            receipt["turnId"] = turn["turn"]["id"]

        return await self._mutate(
            request_id,
            "send_message_to_thread",
            {"threadId": thread_id, "message": message},
            action,
        )

    async def steer_thread(
        self, request_id: str, thread_id: str, expected_turn_id: str, message: str
    ):
        nonempty(thread_id, "thread_id", 128)
        nonempty(expected_turn_id, "expected_turn_id", 128)
        nonempty(message, "message")

        async def action(receipt):
            receipt["threadId"] = thread_id
            receipt["expectedTurnId"] = expected_turn_id
            self.ledger.save(receipt)
            await self._remote_thread(thread_id)
            result = await self.rpc.call(
                "turn/steer",
                {
                    "threadId": thread_id,
                    "expectedTurnId": expected_turn_id,
                    "input": [{"type": "text", "text": message}],
                },
            )
            receipt["turnId"] = result["turnId"]
            if receipt["turnId"] != expected_turn_id:
                raise ValueError(
                    "App Server returned a different turn ID; delivery outcome unknown"
                )

        return await self._mutate(
            request_id,
            "steer_thread",
            {"threadId": thread_id, "expectedTurnId": expected_turn_id, "message": message},
            action,
        )

    async def get_goal(self, thread_id: str):
        nonempty(thread_id, "thread_id", 128)
        return clipped(await self.rpc.call("thread/goal/get", {"threadId": thread_id}), 4000)

    async def list_threads(self, cwd=None, limit=20, cursor=None):
        if not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        params = {"limit": limit, "useStateDbOnly": True}
        if cwd is not None:
            params["cwd"] = self._directory(cwd)
        if cursor is not None:
            params["cursor"] = cursor
        return clipped(await self.rpc.call("thread/list", params), 4000)

    async def read_thread(self, thread_id: str, limit=10, cursor=None, max_text_chars=4000):
        nonempty(thread_id, "thread_id", 128)
        if not 1 <= limit <= 100 or not 100 <= max_text_chars <= 20_000:
            raise ValueError("limit must be 1–100 and max_text_chars must be 100–20000")
        metadata = await self.rpc.call("thread/read", {"threadId": thread_id})
        params = {"threadId": thread_id, "limit": limit, "itemsView": "full"}
        if cursor is not None:
            params["cursor"] = cursor
        page = await self.rpc.call("thread/turns/list", params)
        return clipped(
            {
                "thread": metadata["thread"],
                "turnsPage": page,
                "pendingRequests": self.rpc.pending_for(thread_id),
                "declinedRequests": self.rpc.declined_for(thread_id),
            },
            max_text_chars,
        )

    async def wait_thread(self, thread_id: str, turn_id: str, timeout_seconds=20):
        nonempty(thread_id, "thread_id", 128)
        nonempty(turn_id, "turn_id", 128)
        if not 0 <= timeout_seconds <= 50:
            raise ValueError("timeout_seconds must be between 0 and 50")
        latest = None

        async def inspect():
            # Search recent turns only; never confuse a different completed turn with the target.
            page = await self.rpc.call(
                "thread/turns/list",
                {
                    "threadId": thread_id,
                    "limit": 100,
                    "itemsView": "summary",
                },
            )
            return next((turn for turn in page["data"] if turn["id"] == turn_id), None)

        if timeout_seconds == 0:
            latest = await inspect()
        else:
            try:
                async with asyncio.timeout(timeout_seconds):
                    while True:
                        latest = await inspect()
                        if latest and latest.get("status") in TERMINAL_STATUSES:
                            break
                        # A held question blocks the turn: hand it to the caller right away.
                        if self.rpc.pending_for(thread_id, turn_id):
                            break
                        await self.rpc.wait_held_change(0.5)
            except TimeoutError:
                pass
        terminal = latest is not None and latest.get("status") in TERMINAL_STATUSES
        pending = [] if terminal else self.rpc.pending_for(thread_id, turn_id)
        return clipped(
            {
                "threadId": thread_id,
                "turnId": turn_id,
                "timedOut": not terminal and not pending,
                "waitingForAnswer": bool(pending),
                "turn": latest,
                "observation": "found" if latest else "not observed in latest 100 turns",
                "pendingRequests": pending,
                **(
                    {
                        "note": "The turn is blocked on pendingRequests. Answer each with "
                        "answer_request (or escalate the question to the user); unanswered "
                        "requests are auto-declined at their deadline."
                    }
                    if pending
                    else {}
                ),
            },
            4000,
        )

    # --- held question-type requests (references/upstream-sync.md, patch 4) ----------------------

    async def answer_request(self, request_id: str, answers=None, decline: bool = False):
        """Answer or decline one held requestUserInput / MCP elicitation request."""
        nonempty(request_id, "request_id", 128)
        entry = self.rpc.held.get(request_id)
        if entry is None:
            raise ValueError(
                f"No pending request {request_id!r}: it was already answered, timed out "
                "(see declinedRequests in get_turn_result / read_thread), was resolved by the "
                "App Server, or the bridge restarted. Held requests are not persisted."
            )
        if decline and answers is not None:
            raise ValueError("Pass either answers or decline=true, not both")
        if not decline and answers is None:
            raise ValueError("Pass answers (or decline=true) to settle the request")
        result = self._response_for(entry, answers, decline)
        await self.rpc.answer(request_id, result)
        return {
            "requestId": request_id,
            "type": entry["type"],
            "threadId": entry["threadId"],
            "turnId": entry["turnId"],
            "sent": "decline" if decline else "answers",
            "note": "Sent to the App Server. Use wait_thread / get_turn_result to follow the turn.",
        }

    @staticmethod
    def _response_for(entry, answers, decline):
        """Build the protocol response for a held request, validating `answers` by request type."""
        params = entry["params"]
        if entry["type"] == "userInput":
            if decline:
                return {"answers": {}}
            if not isinstance(answers, dict) or not answers:
                raise ValueError(
                    "answers must map each question id to a string or a list of strings"
                )
            questions = [q for q in params.get("questions") or [] if isinstance(q, dict)]
            known = {q.get("id") for q in questions}
            unknown = sorted(set(answers) - known)
            if unknown:
                raise ValueError(f"Unknown question id(s) {unknown}; expected {sorted(known)}")
            missing = sorted(known - set(answers))
            if missing:
                raise ValueError(f"Missing answer(s) for question id(s) {missing}")
            normalized = {}
            for question_id, value in answers.items():
                values = [value] if isinstance(value, str) else value
                if (
                    not isinstance(values, list)
                    or not values
                    or any(not isinstance(v, str) or not v.strip() for v in values)
                ):
                    raise ValueError(
                        f"Answer for {question_id!r} must be a non-empty string or list of "
                        "non-empty strings"
                    )
                normalized[question_id] = {"answers": values}
            return {"answers": normalized}
        # mcpServer/elicitation/request
        mode = params.get("mode")
        if decline:
            return {"action": "decline"}
        if mode == "openai/userVerification":
            raise ValueError(
                "This is a device-authenticated verification; the bridge cannot produce the "
                "proof. Use decline=true or have the user approve it in a Codex client."
            )
        if not isinstance(answers, dict):
            raise ValueError("answers must be an object (the elicitation content)")
        if mode == "url":
            if answers:
                raise ValueError("A url-mode elicitation takes answers={} (accept) or decline=true")
            return {"action": "accept"}
        schema = params.get("requestedSchema")
        if isinstance(schema, dict) and isinstance(schema.get("properties"), dict):
            unknown = sorted(set(answers) - set(schema["properties"]))
            if unknown:
                raise ValueError(
                    f"Unknown field(s) {unknown}; expected {sorted(schema['properties'])}"
                )
            required = schema.get("required") if isinstance(schema.get("required"), list) else []
            missing = sorted(set(required) - set(answers))
            if missing:
                raise ValueError(f"Missing required field(s) {missing}")
        return {"action": "accept", "content": answers}

    # --- charm-codex additions (references/upstream-sync.md, patch 3) ---------------------------

    async def interrupt_turn(self, thread_id: str, turn_id: str):
        """Ask the App Server to interrupt one turn. Idempotent; no ledger receipt."""
        nonempty(thread_id, "thread_id", 128)
        nonempty(turn_id, "turn_id", 128)
        await self._remote_thread(thread_id)
        result = await self.rpc.call("turn/interrupt", {"threadId": thread_id, "turnId": turn_id})
        return {
            "threadId": thread_id,
            "turnId": turn_id,
            "interruptRequested": True,
            "result": result,
            "note": "Accepted means requested; poll wait_thread for status interrupted.",
        }

    async def _find_turn(self, thread_id, turn_id, pages=5):
        cursor = None
        for _ in range(pages):
            params = {"threadId": thread_id, "limit": 100, "itemsView": "full"}
            if cursor is not None:
                params["cursor"] = cursor
            page = await self.rpc.call("thread/turns/list", params)
            for turn in page.get("data", []):
                if turn.get("id") == turn_id:
                    return turn
            cursor = page.get("nextCursor")
            if not cursor:
                break
        return None

    async def get_turn_result(self, thread_id: str, turn_id: str, max_text_chars: int = 20_000):
        """Final agent message, fileChange diffs, command summary and declines of one turn."""
        nonempty(thread_id, "thread_id", 128)
        nonempty(turn_id, "turn_id", 128)
        if not 100 <= max_text_chars <= 200_000:
            raise ValueError("max_text_chars must be 100–200000")
        await self._remote_thread(thread_id)
        turn = await self._find_turn(thread_id, turn_id)
        declined = self.rpc.declined_for(thread_id, turn_id)
        pending = self.rpc.pending_for(thread_id, turn_id)
        if turn is None:
            return {
                "threadId": thread_id,
                "turnId": turn_id,
                "found": False,
                "observation": "not found in the newest 500 turns",
                "declinedRequests": declined,
                "pendingRequests": pending,
            }
        items = [item for item in turn.get("items") or [] if isinstance(item, dict)]

        def cut(value):
            if isinstance(value, str) and len(value) > max_text_chars:
                return (
                    value[:max_text_chars]
                    + f"\n[truncated; original length {len(value)} characters]"
                )
            return value

        messages = [item for item in items if item.get("type") == "agentMessage"]
        file_changes = []
        for item in items:
            if item.get("type") != "fileChange":
                continue
            changes = item.get("changes")
            file_changes.append(
                {
                    "id": item.get("id"),
                    "status": item.get("status"),
                    "changes": [
                        {
                            "path": change.get("path"),
                            "kind": change.get("kind"),
                            "diff": cut(change.get("diff")),
                        }
                        if isinstance(change, dict)
                        else change
                        for change in (changes if isinstance(changes, list) else [])
                    ],
                }
            )
        commands = [
            {
                "command": cut(item.get("command")),
                "status": item.get("status"),
                "exitCode": item.get("exitCode"),
            }
            for item in items
            if item.get("type") == "commandExecution"
        ]
        status = turn.get("status")
        return {
            "threadId": thread_id,
            "turnId": turn_id,
            "found": True,
            "status": status,
            "terminal": status in TERMINAL_STATUSES,
            "error": turn.get("error"),
            "finalAgentMessage": cut(messages[-1].get("text")) if messages else None,
            "fileChanges": file_changes,
            "commands": commands,
            "declinedRequests": declined,
            "pendingRequests": pending,
            "note": "fileChange diffs are the inner agent's account; verify the real working "
            "tree diff independently."
            + (
                " declinedRequests lists approvals/questions the bridge refused (category "
                "approval = never allowed, question = timed out or turn ended unanswered)."
                if declined
                else ""
            )
            + (" pendingRequests are waiting for answer_request." if pending else ""),
        }
