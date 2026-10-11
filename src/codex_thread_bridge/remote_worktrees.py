"""Remote-mode Git worktree helpers: strict validation, fixed argv, command/exec sandboxes.

Everything here is pure (no I/O). The Bridge sends the argv through the App Server's
`command/exec`, which runs inside the remote host's sandbox. No shell string is ever built and
no caller-supplied argv is accepted: only the fixed `git worktree` forms below exist.
"""

import posixpath
import re
from pathlib import PurePosixPath

NAME_PATTERN = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}")
WORKTREES_DIR = ".worktrees"
FULL_SHA = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")
REF_FORBIDDEN = re.compile(r"[\x00-\x20\x7f~^:?*\[\\]")
GIT_ENV = {"GIT_TERMINAL_PROMPT": "0", "GIT_OPTIONAL_LOCKS": "0"}


def validate_name(name) -> str:
    if not isinstance(name, str) or not NAME_PATTERN.fullmatch(name):
        raise ValueError("name must match ^[a-z0-9][a-z0-9._-]{0,63}$")
    if name.endswith(".") or name.endswith(".lock") or ".." in name:
        raise ValueError("name must not end with '.' or '.lock' or contain '..'")
    return name


def validate_ref(value, what: str = "ref") -> str:
    """`git check-ref-format` rules, plus: no leading '-', no revision suffixes (^ ~ @{})."""
    if not isinstance(value, str) or not value or len(value) > 200:
        raise ValueError(f"{what} must contain 1-200 characters")
    if value.startswith("-"):
        raise ValueError(f"{what} must not start with '-'")
    if value == "@":
        raise ValueError(f"{what} must not be '@'")
    if REF_FORBIDDEN.search(value):
        raise ValueError(f"{what} contains a forbidden character")
    if ".." in value or "@{" in value or "//" in value:
        raise ValueError(f"{what} must not contain '..', '@{{' or '//'")
    if value.startswith("/") or value.endswith("/") or value.endswith("."):
        raise ValueError(f"{what} must not start/end with '/' or end with '.'")
    for part in value.split("/"):
        if part.startswith(".") or part.endswith(".lock"):
            raise ValueError(f"{what} components must not start with '.' or end with '.lock'")
    return value


def validate_branch(value) -> str:
    validate_ref(value, "branch")
    if value == "HEAD" or value.startswith("refs/") or FULL_SHA.fullmatch(value):
        raise ValueError("branch must be a short branch name (not HEAD, refs/..., or a commit ID)")
    return value


def worktrees_dir(root: str) -> str:
    return posixpath.join(root, WORKTREES_DIR)


def worktree_path(root: str, name: str) -> str:
    return posixpath.join(root, WORKTREES_DIR, validate_name(name))


def is_managed(root: str, path: str) -> bool:
    """True if `path` is directly under <root>/.worktrees/."""
    return PurePosixPath(path).parent == PurePosixPath(worktrees_dir(root))


def git_argv(root: str, *args: str) -> list[str]:
    return ["git", "-C", root, *args]


def add_argv(root: str, path: str, base_ref: str, branch: str | None) -> list[str]:
    mode = ["-b", branch, "--no-track"] if branch else ["--detach"]
    return git_argv(root, "worktree", "add", "--quiet", *mode, "--", path, base_ref)


def remove_argv(root: str, path: str, force: bool) -> list[str]:
    return git_argv(root, "worktree", "remove", *(["--force"] if force else []), "--", path)


def list_argv(root: str) -> list[str]:
    return git_argv(root, "worktree", "list", "--porcelain")


def head_argv(path: str) -> list[str]:
    return git_argv(path, "rev-parse", "HEAD")


def write_sandbox(root: str) -> dict:
    """Minimal workspaceWrite policy: only the Git directory and the worktrees directory."""
    return {
        "type": "workspaceWrite",
        "writableRoots": [posixpath.join(root, ".git"), worktrees_dir(root)],
        "networkAccess": False,
        "excludeSlashTmp": True,
        "excludeTmpdirEnvVar": True,
    }


READ_SANDBOX = {"type": "readOnly", "networkAccess": False}


def parse_porcelain(text: str, root: str) -> list[dict]:
    entries, current = [], None
    for line in text.splitlines() + [""]:
        if not line:
            if current:
                entries.append(current)
            current = None
            continue
        key, _, value = line.partition(" ")
        if key == "worktree":
            current = {"path": value, "managed": is_managed(root, value), "detached": False}
        elif current is not None:
            if key == "HEAD":
                current["head"] = value
            elif key == "branch":
                current["branch"] = value
            elif key in ("detached", "bare"):
                current[key] = True
            elif key in ("locked", "prunable"):
                current[key] = value or True
    return entries
