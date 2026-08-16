"""Tool layer (roadmap step 10, part 1 — no LLM involved). Binds a task's allow-listed tool *families*
(`ctx.tools`, validated by rule 4 against `mas/planner/capabilities.py`) to implementations.

Trust boundary (invariants I-7, I-11; antipatterns B12) — two different mechanisms, stated precisely:
- **filesystem tools are path-jailed in-process**: every path argument is untrusted model output, resolved inside the
  attempt's worktree — no absolute paths, no `..`, no symlink escape, never `.git/` (the runtime owns commits), never
  writes under `acceptance/` (I-3); reads may be narrowed further by `context_spec.paths` globs;
- **command tools (shell / python / pytest) never run here**: they go through an `ExecutionBackend`
  (`mas/workers/execution.py`). Without a backend the tools are not even offered. `SandboxExecutionBackend` confines a
  command to a per-attempt container that sees exactly the worktree (no host paths, no shared /data, no network,
  read-only rootfs, non-root, resource limits, container-side kill timeout, removed at close). `LocalExecutionBackend`
  is *test-only* (`unsafe_ok=True`): bounded — time, output cap, cancel, process-tree kill — but not confined;
- git tools are read-only, fixed-argv host commands with jailed path arguments (`git status --short`, `git diff`);
- tool output is *data*: returned as text for the agent to hand back as a tool result — nothing here interprets it.
  `model` is not a tool (that is `ctx.model`).

Families → model-facing tools:
    filesystem → read_file, write_file, list_files      shell → run_command          (backend)
    python     → run_python, run_pytest (backend)       git   → git_status, git_diff (read-only, host, fixed argv)
"""

from __future__ import annotations

import fnmatch
import logging
import math
import shutil
import tempfile
import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

from mas.planner.capabilities import KNOWN_TOOLS, TOOL_FILESYSTEM, TOOL_GIT, TOOL_PYTHON, TOOL_SHELL
from mas.workers.execution import (  # noqa: F401 - re-exported for callers/tests
    CommandResult,
    ExecutionBackend,
    LocalExecutionBackend,
    SandboxExecutionBackend,
    kill_tree,
    run_bounded,
    sanitized_env,
)

log = logging.getLogger(__name__)


class ToolError(Exception):
    """A tool refused or failed; the message is safe to hand back to the model as an error tool result."""


class ToolDenied(ToolError):
    """The tool family is not granted to this task (or the tool does not exist)."""


class PathJailError(ToolError):
    """A path argument tried to leave the worktree or touch a reserved location."""


@dataclass(frozen=True)
class ToolLimits:
    command_timeout_s: float = 60.0  # per command; clamped to the attempt's remaining runtime
    max_output_bytes: int = 64_000  # stdout+stderr kept per command (the process is killed on overflow)
    max_file_bytes: int = 512_000  # read/write size cap
    max_list_entries: int = 2_000
    kill_grace_s: float = 2.0


RESERVED_PREFIXES: tuple[str, ...] = (".git",)  # never readable or writable through tools
WRITE_DENIED_PREFIXES: tuple[str, ...] = ("acceptance",)  # read-only to workers (I-3, ADR-007)

TOOLS_BY_FAMILY: dict[str, tuple[str, ...]] = {
    TOOL_FILESYSTEM: ("read_file", "write_file", "list_files"),
    TOOL_SHELL: ("run_command",),
    TOOL_PYTHON: ("run_python", "run_pytest"),
    TOOL_GIT: ("git_status", "git_diff"),
}
FAMILY_OF_TOOL: dict[str, str] = {t: fam for fam, ts in TOOLS_BY_FAMILY.items() for t in ts}


# ----------------------------------------------------------------------------- jail


class Jail:
    """Resolves model-supplied relative paths inside `root`; rejects everything that would leave it."""

    def __init__(self, root: Path, *, read_globs: Iterable[str] | None = None):
        self.root = Path(root).resolve()
        self.read_globs = [g for g in (read_globs or []) if g]

    def resolve(self, rel: Any, *, for_write: bool = False) -> Path:
        if not isinstance(rel, str) or not rel.strip():
            raise PathJailError("path must be a non-empty relative string")
        raw = rel.strip().replace("\\", "/")
        p = PurePosixPath(raw)
        if p.is_absolute() or raw.startswith("//") or (len(raw) > 1 and raw[1] == ":"):
            raise PathJailError(f"absolute paths are not allowed: {rel!r}")
        if any(part == ".." for part in p.parts):
            raise PathJailError(f"'..' is not allowed: {rel!r}")
        full = (self.root / raw).resolve(strict=False)  # follows existing symlinks → an escape resolves outside
        if not full.is_relative_to(self.root):
            raise PathJailError(f"path escapes the worktree: {rel!r}")
        parts = full.relative_to(self.root).parts
        if parts and parts[0] in RESERVED_PREFIXES:
            raise PathJailError(f"{parts[0]}/ is reserved (the runtime owns it): {rel!r}")
        if for_write and parts and parts[0] in WRITE_DENIED_PREFIXES:
            raise PathJailError(f"{parts[0]}/ is read-only to workers: {rel!r}")
        if not for_write and self.read_globs and parts:
            relposix = "/".join(parts)
            if not any(fnmatch.fnmatchcase(relposix, g) or relposix.startswith(g.rstrip("/*") + "/") for g in self.read_globs):
                raise PathJailError(f"outside the task's context paths {self.read_globs}: {rel!r}")
        return full


# ----------------------------------------------------------------------------- the layer


@dataclass
class ToolResult:
    content: str
    is_error: bool = False


@dataclass
class ToolCall:
    """A tool invocation record (for tests / the agent's transcript)."""

    name: str
    input: dict[str, Any]
    result: ToolResult
    duration_s: float = 0.0
    meta: dict[str, Any] = field(default_factory=dict)


class ToolLayer:
    """The only way an agent acts on the world. Construct one per attempt. Command tools exist only when an
    `ExecutionBackend` is supplied (fail closed); the runtime supplies a sandbox, tests may pass a local one."""

    def __init__(
        self,
        root: Path,
        granted: Iterable[str],
        *,
        backend: ExecutionBackend | None = None,
        read_globs: Iterable[str] | None = None,
        limits: ToolLimits | None = None,
        cancel: threading.Event | None = None,
        deadline: float | None = None,  # time.monotonic() value: the attempt's runtime end; commands never outlive it
        close_backend: bool = True,
    ):
        self.jail = Jail(root, read_globs=read_globs)
        self.root = self.jail.root
        self.granted = frozenset(granted) & KNOWN_TOOLS
        self.backend = backend
        self._close_backend = close_backend
        self.limits = limits or ToolLimits()
        self.cancel = cancel or threading.Event()
        self.deadline = deadline
        self._home = Path(tempfile.mkdtemp(prefix="mas-tool-home-"))  # HOME/TMP for the host-side git commands
        self.calls: list[ToolCall] = []

    # ------------------------------------------------------------------ lifecycle

    def close(self) -> None:
        if self.backend is not None and self._close_backend:
            try:
                self.backend.close()
            except Exception:
                log.warning("execution backend close failed", exc_info=True)
        shutil.rmtree(self._home, ignore_errors=True)

    def __enter__(self) -> ToolLayer:
        return self

    def __exit__(self, *a: Any) -> None:
        self.close()

    # ------------------------------------------------------------------ what the model sees

    def _family_available(self, fam: str) -> bool:
        if fam not in self.granted:
            return False
        if fam in (TOOL_SHELL, TOOL_PYTHON) and self.backend is None:
            return False  # no confined execution for this attempt -> the tools do not exist for the model
        return True

    def tool_names(self) -> list[str]:
        return [t for fam in TOOLS_BY_FAMILY for t in TOOLS_BY_FAMILY[fam] if self._family_available(fam)]

    def schemas(self) -> list[dict[str, Any]]:
        """Provider-neutral tool definitions for the available tools only."""
        return [SCHEMAS[name] for name in self.tool_names()]

    # ------------------------------------------------------------------ dispatch

    def dispatch(self, name: str, input: dict[str, Any] | None) -> ToolResult:
        """Run one tool. Denials and failures come back as error results (data for the model), never exceptions —
        except programming errors, which propagate."""
        t0 = time.monotonic()
        args = dict(input or {})
        try:
            fam = FAMILY_OF_TOOL.get(name)
            if fam is None:
                raise ToolDenied(f"unknown tool {name!r}; available: {self.tool_names()}")
            if fam not in self.granted:
                raise ToolDenied(f"tool {name!r} ({fam}) is not granted to this task; available: {self.tool_names()}")
            if fam in (TOOL_SHELL, TOOL_PYTHON) and self.backend is None:
                raise ToolDenied(f"tool {name!r} needs a confined execution backend and this attempt has none")
            handler = getattr(self, f"_t_{name}")
            res = ToolResult(content=handler(**args))
        except ToolError as e:
            res = ToolResult(content=f"error: {e}", is_error=True)
        except TypeError as e:  # wrong/missing arguments from the model
            res = ToolResult(content=f"error: bad arguments for {name}: {e}", is_error=True)
        self.calls.append(ToolCall(name=name, input=args, result=res, duration_s=round(time.monotonic() - t0, 3)))
        return res

    # ------------------------------------------------------------------ filesystem

    def _t_read_file(self, path: str) -> str:
        p = self.jail.resolve(path)
        if not p.is_file():
            raise ToolError(f"not a file: {path!r}")
        if p.stat().st_size > self.limits.max_file_bytes:
            raise ToolError(f"file too large ({p.stat().st_size} bytes > {self.limits.max_file_bytes}): {path!r}")
        return p.read_text(encoding="utf-8", errors="replace")

    def _t_write_file(self, path: str, content: str) -> str:
        if not isinstance(content, str):
            raise ToolError("content must be a string")
        if len(content.encode("utf-8")) > self.limits.max_file_bytes:
            raise ToolError(f"content too large (> {self.limits.max_file_bytes} bytes)")
        p = self.jail.resolve(path, for_write=True)
        if p.exists() and p.is_dir():
            raise ToolError(f"is a directory: {path!r}")
        p.parent.mkdir(parents=True, exist_ok=True)
        # re-check after mkdir: a parent could have been a symlink created meanwhile
        self.jail.resolve(path, for_write=True)
        p.write_text(content, encoding="utf-8", newline="\n")
        return f"wrote {len(content)} chars to {path}"

    def _t_list_files(self, path: str = ".", recursive: bool = False) -> str:
        p = self.jail.resolve(path)
        if not p.is_dir():
            raise ToolError(f"not a directory: {path!r}")
        entries: list[str] = []
        it = p.rglob("*") if recursive else p.iterdir()
        for child in sorted(it):
            rel = child.relative_to(self.root).as_posix()
            if rel.split("/", 1)[0] in RESERVED_PREFIXES:
                continue
            entries.append(rel + ("/" if child.is_dir() else ""))
            if len(entries) >= self.limits.max_list_entries:
                entries.append(f"[truncated at {self.limits.max_list_entries} entries]")
                break
        return "\n".join(entries) if entries else "(empty)"

    # ------------------------------------------------------------------ commands (through the execution backend)

    def _timeout(self, requested: Any) -> float:
        """Model-supplied timeout: must be a finite positive number (JSON parsers accept NaN/Infinity; bools are ints);
        anything else is a tool error, and the value is capped by the layer's limit and the attempt deadline."""
        if requested is None:
            t = self.limits.command_timeout_s
        else:
            if isinstance(requested, bool) or not isinstance(requested, int | float) or not math.isfinite(requested):
                raise ToolError(f"timeout_s must be a finite number, got {requested!r}")
            if requested <= 0:
                raise ToolError(f"timeout_s must be positive, got {requested!r}")
            t = min(float(requested), self.limits.command_timeout_s)
        if self.deadline is not None:
            t = min(t, max(0.05, self.deadline - time.monotonic()))
        return t

    def _guard(self) -> None:
        if self.cancel.is_set():
            raise ToolError("attempt cancelled; no more commands")
        if self.deadline is not None and time.monotonic() >= self.deadline:
            raise ToolError("attempt runtime exhausted; no more commands")

    def _t_run_command(self, command: str, timeout_s: float | None = None) -> str:
        if not isinstance(command, str) or not command.strip():
            raise ToolError("command must be a non-empty string")
        self._guard()
        assert self.backend is not None
        return self.backend.run_shell(command, timeout_s=self._timeout(timeout_s), cancel=self.cancel).render()

    def _t_run_python(self, code: str, timeout_s: float | None = None) -> str:
        if not isinstance(code, str) or not code.strip():
            raise ToolError("code must be a non-empty string")
        self._guard()
        assert self.backend is not None
        return self.backend.run(["python", "-c", code], timeout_s=self._timeout(timeout_s), cancel=self.cancel).render()

    def _t_run_pytest(self, args: list[str] | None = None, timeout_s: float | None = None) -> str:
        extra = [str(a) for a in (args or [])]
        for a in extra:
            if a.startswith("-") and a.split("=", 1)[0] in {"--rootdir", "-c", "--confcutdir", "-p"}:
                raise ToolError(f"pytest option not allowed: {a}")
        self._guard()
        assert self.backend is not None
        argv = ["python", "-m", "pytest", "-q", "-p", "no:cacheprovider", *extra]
        return self.backend.run(argv, timeout_s=self._timeout(timeout_s), cancel=self.cancel).render()

    # ------------------------------------------------------------------ git (read-only, fixed argv, host side)

    def _git(self, *args: str) -> str:
        self._guard()
        res = run_bounded(
            ["git", *args],
            cwd=self.root,
            env=sanitized_env(self._home),
            timeout_s=self._timeout(30),
            max_output_bytes=self.limits.max_output_bytes,
            cancel=self.cancel,
        )
        return res.render()

    def _t_git_status(self) -> str:
        return self._git("status", "--short")

    def _t_git_diff(self, path: str | None = None) -> str:
        if path is not None:
            self.jail.resolve(path)
            return self._git("diff", "--", path)
        return self._git("diff")


SCHEMAS: dict[str, dict[str, Any]] = {
    "read_file": {
        "name": "read_file",
        "description": "Read a UTF-8 text file inside the worktree. Path is relative to the worktree root.",
        "input_schema": {"type": "object", "properties": {"path": {"type": "string"}}, "required": ["path"]},
    },
    "write_file": {
        "name": "write_file",
        "description": "Create or overwrite a text file inside the worktree (parent directories are created). "
        "Relative path only; .git/ and acceptance/ are off limits.",
        "input_schema": {
            "type": "object",
            "properties": {"path": {"type": "string"}, "content": {"type": "string"}},
            "required": ["path", "content"],
        },
    },
    "list_files": {
        "name": "list_files",
        "description": "List entries of a directory inside the worktree (default: the root). Directories end with '/'.",
        "input_schema": {
            "type": "object",
            "properties": {"path": {"type": "string", "default": "."}, "recursive": {"type": "boolean", "default": False}},
        },
    },
    "run_command": {
        "name": "run_command",
        "description": "Run a shell command in the worktree root with a sanitized environment, a hard timeout and an output "
        "cap. No network is available. Returns exit code and combined stdout/stderr.",
        "input_schema": {
            "type": "object",
            "properties": {"command": {"type": "string"}, "timeout_s": {"type": "number"}},
            "required": ["command"],
        },
    },
    "run_python": {
        "name": "run_python",
        "description": "Run a Python snippet (python -c) in the worktree root under the same limits as run_command.",
        "input_schema": {
            "type": "object",
            "properties": {"code": {"type": "string"}, "timeout_s": {"type": "number"}},
            "required": ["code"],
        },
    },
    "run_pytest": {
        "name": "run_pytest",
        "description": "Run pytest -q in the worktree root (optional extra args such as a test path or -k expression).",
        "input_schema": {
            "type": "object",
            "properties": {"args": {"type": "array", "items": {"type": "string"}}, "timeout_s": {"type": "number"}},
        },
    },
    "git_status": {
        "name": "git_status",
        "description": "git status --short of the worktree (read-only; the runtime commits your work when you finish).",
        "input_schema": {"type": "object", "properties": {}},
    },
    "git_diff": {
        "name": "git_diff",
        "description": "git diff of the worktree, optionally limited to one relative path (read-only).",
        "input_schema": {"type": "object", "properties": {"path": {"type": "string"}}},
    },
}
