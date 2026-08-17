"""Execution boundary for command tools (roadmap step 10).

`cwd=<worktree>` plus a clean environment *bounds* a subprocess (time, output, cancellation, process tree) but does not
*confine* it: a model-authored command can still touch absolute paths, other attempts' worktrees under a shared
`/data`, or the network. So command tools never run directly — they go through an `ExecutionBackend`:

    LLM worker ── read/write/list ──▶ in-process path Jail (tools.py)
               ── shell/python  ──▶ ExecutionBackend
                                       ├── LocalExecutionBackend    TEST-ONLY (`unsafe_ok=True`): bounded, NOT confined
                                       └── SandboxExecutionBackend  per-attempt container: exact worktree mounted RW at /work,
                                           nothing else from the host, --network none, --read-only rootfs, tmpfs /tmp,
                                           non-root, --cap-drop ALL, no-new-privileges, pids/memory/cpu limits,
                                           container-side `timeout -s KILL` per command, container removed at close

`run_bounded` is the shared primitive (hard timeout, output cap that kills the flood, cooperative cancel, process-tree
termination) — used by the local backend, by the sandbox backend to drive the `docker` client, and by tools.py for
its fixed-argv read-only git commands. Whoever owns Docker access constructs the sandbox backend; hardened compose
workers have no docker.sock, so for them the construction moves to a trusted execution-runner service (the verifier
service pattern) that resolves the worktree from the attempt identity — the backend seam is the same.
"""

from __future__ import annotations

import logging
import math
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

log = logging.getLogger(__name__)


class ExecutionError(RuntimeError):
    """The backend itself failed (cannot start the sandbox, docker missing, ...). Distinct from a command failing."""


class ExecutionUnavailable(ExecutionError):
    """No confined execution is available for this attempt (fail closed: command tools are not offered)."""


# ----------------------------------------------------------------------------- bounded subprocess primitive


@dataclass
class CommandResult:
    exit_code: int | None
    output: str
    duration_s: float
    timed_out: bool = False
    truncated: bool = False
    cancelled: bool = False
    abandoned: bool = False  # the execution runner died mid-command; the command was NOT replayed (side effects unknown)
    error: str | None = None  # the execution machinery itself failed / refused (not the command's own exit status)

    def render(self) -> str:
        head = f"exit_code={self.exit_code}"
        if self.timed_out:
            head += " TIMED_OUT"
        if self.cancelled:
            head += " CANCELLED"
        if self.abandoned:
            head += " ABANDONED"
        if self.truncated:
            head += " OUTPUT_TRUNCATED"
        if self.error:
            head += f" ERROR: {self.error}"
        return f"{head} ({self.duration_s:.1f}s)\n{self.output}"

    def as_dict(self, *, with_output: bool = False) -> dict[str, Any]:
        d: dict[str, Any] = {
            "exit_code": self.exit_code,
            "duration_s": self.duration_s,
            "timed_out": self.timed_out,
            "truncated": self.truncated,
            "cancelled": self.cancelled,
            "abandoned": self.abandoned,
            "error": self.error,
        }
        if with_output:
            d["output"] = self.output
        return d


def kill_tree(proc: subprocess.Popen) -> None:
    """Terminate the process and everything it spawned (process group / session; `taskkill /T` on Windows)."""
    try:
        if os.name == "nt":
            subprocess.run(["taskkill", "/PID", str(proc.pid), "/T", "/F"], capture_output=True, timeout=15, check=False)
        else:
            import signal

            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except Exception:
        log.debug("kill tree fallback for pid %s", proc.pid, exc_info=True)
    try:
        proc.kill()
    except Exception:
        pass


# Environment allow-list for local subprocesses. Nothing else from the worker's environment leaks (no keys, no MAS_*).
_ENV_ALLOW = ("PATH", "SYSTEMROOT", "SYSTEMDRIVE", "COMSPEC", "PATHEXT", "WINDIR", "LANG", "LC_ALL", "TZ")


def sanitized_env(home: Path, extra: dict[str, str] | None = None) -> dict[str, str]:
    env = {k: os.environ[k] for k in _ENV_ALLOW if k in os.environ}
    env["HOME"] = env["USERPROFILE"] = str(home)
    env["TMP"] = env["TEMP"] = env["TMPDIR"] = str(home)
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["PYTHONUTF8"] = "1"
    env["NO_COLOR"] = "1"
    env["GIT_TERMINAL_PROMPT"] = "0"
    if extra:
        env.update(extra)
    return env


def run_bounded(
    argv: list[str] | str,
    *,
    cwd: Path,
    env: dict[str, str],
    timeout_s: float,
    max_output_bytes: int,
    cancel: threading.Event | None = None,
    shell: bool = False,
    kill_grace_s: float = 2.0,
) -> CommandResult:
    """Run a command with a hard timeout, an output cap, cooperative cancellation and process-tree termination."""
    t0 = time.monotonic()
    popen_kwargs: dict[str, Any] = dict(
        cwd=str(cwd), env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL
    )
    if os.name == "nt":
        popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP  # type: ignore[attr-defined]
    else:
        popen_kwargs["start_new_session"] = True
    proc = subprocess.Popen(argv, shell=shell, **popen_kwargs)  # noqa: S603 - bounded on purpose; confinement is the backend's job
    chunks: list[bytes] = []
    size = 0
    flags = {"truncated": False, "timed_out": False, "cancelled": False}
    done = threading.Event()

    def _reader() -> None:
        nonlocal size
        try:
            assert proc.stdout is not None
            while True:
                chunk = proc.stdout.read(4096)
                if not chunk:
                    break
                room = max_output_bytes - size
                if room > 0:
                    keep = chunk[:room]
                    chunks.append(keep)
                    size += len(keep)
                if size >= max_output_bytes:
                    if len(chunk) > room or proc.stdout.read(1):  # anything beyond the cap → stop the flood at the source
                        flags["truncated"] = True
                        kill_tree(proc)
                        while proc.stdout.read(65536):  # drain so the dying process never blocks on a full pipe
                            pass
                    break
        except Exception:
            log.debug("reader ended", exc_info=True)
        finally:
            done.set()

    threading.Thread(target=_reader, name="exec-reader", daemon=True).start()
    deadline = t0 + max(0.05, timeout_s)
    while True:
        if proc.poll() is not None:
            done.wait(kill_grace_s)  # process ended: let the reader drain what is left
            break
        if cancel is not None and cancel.is_set():
            flags["cancelled"] = True
            kill_tree(proc)
            break
        if time.monotonic() >= deadline:
            flags["timed_out"] = True
            kill_tree(proc)
            break
        time.sleep(0.05)
    try:
        proc.wait(timeout=kill_grace_s)
    except subprocess.TimeoutExpired:
        kill_tree(proc)
        try:
            proc.wait(timeout=kill_grace_s)
        except subprocess.TimeoutExpired:
            log.warning("process %s did not die after kill", proc.pid)
    done.wait(kill_grace_s)
    try:
        if proc.stdout is not None:
            proc.stdout.close()
    except Exception:
        pass
    out = b"".join(chunks).decode("utf-8", "replace")
    if flags["truncated"]:
        out += "\n[output truncated]"
    return CommandResult(
        exit_code=proc.returncode,
        output=out,
        duration_s=round(time.monotonic() - t0, 3),
        timed_out=flags["timed_out"],
        truncated=flags["truncated"],
        cancelled=flags["cancelled"],
    )


# ----------------------------------------------------------------------------- the boundary


class ExecutionBackend(Protocol):
    name: str
    confined: bool  # True only when commands cannot see anything but the attempt's worktree

    def run(self, argv: list[str], *, timeout_s: float, cancel: threading.Event | None = None) -> CommandResult:
        """Exec-style command (no shell). `argv[0] == "python"` means the backend's Python."""
        ...

    def run_shell(self, command: str, *, timeout_s: float, cancel: threading.Event | None = None) -> CommandResult:
        """Shell command line."""
        ...

    def close(self) -> None: ...


class LocalExecutionBackend:
    """TEST-ONLY. Runs commands directly on the host in the worktree: bounded (time, output, cancel, process tree),
    environment sanitized — but NOT confined (absolute paths, other worktrees and the network are reachable).
    Construction requires `unsafe_ok=True`; nothing in the runtime or CLI passes it."""

    name = "local-unconfined"
    confined = False

    def __init__(self, root: Path, *, unsafe_ok: bool = False, max_output_bytes: int = 64_000, kill_grace_s: float = 2.0):
        if not unsafe_ok:
            raise ExecutionUnavailable(
                "LocalExecutionBackend runs model-authored commands unconfined on the host; it exists for tests only "
                "(pass unsafe_ok=True). Real workers use SandboxExecutionBackend."
            )
        self.root = Path(root).resolve()
        self.max_output_bytes = max_output_bytes
        self.kill_grace_s = kill_grace_s
        self._home = Path(tempfile.mkdtemp(prefix="mas-local-home-"))

    def _env(self) -> dict[str, str]:
        return sanitized_env(self._home)

    def run(self, argv: list[str], *, timeout_s: float, cancel: threading.Event | None = None) -> CommandResult:
        argv = list(argv)
        if argv and argv[0] == "python":
            argv[0] = sys.executable
        return run_bounded(
            argv,
            cwd=self.root,
            env=self._env(),
            timeout_s=timeout_s,
            max_output_bytes=self.max_output_bytes,
            cancel=cancel,
            kill_grace_s=self.kill_grace_s,
        )

    def run_shell(self, command: str, *, timeout_s: float, cancel: threading.Event | None = None) -> CommandResult:
        return run_bounded(
            command,
            cwd=self.root,
            env=self._env(),
            timeout_s=timeout_s,
            max_output_bytes=self.max_output_bytes,
            cancel=cancel,
            shell=True,
            kill_grace_s=self.kill_grace_s,
        )

    def close(self) -> None:
        shutil.rmtree(self._home, ignore_errors=True)

    def identity(self) -> dict[str, Any]:
        return {"backend": self.name, "confined": False, "python": sys.executable}


@dataclass(frozen=True)
class SandboxSpec:
    image: str = "mas-verifier:latest"  # python + pytest + sh + coreutils `timeout`, non-root; same image the verifier uses
    cpus: float = 1.0
    memory_mb: int = 512
    pids: int = 256
    tmpfs_mb: int = 256
    max_life_s: int = 1800  # container-side outer bound: `sleep max_life_s` is PID 1's child; an orphaned sandbox ends by itself
    max_output_bytes: int = 64_000
    docker: str = "docker"
    user: str | None = (
        None  # default: the current uid:gid on POSIX (so the worktree stays writable/committable), 10001 on Windows
    )


def sandbox_spec_from_settings(cfg: Any = None) -> SandboxSpec:
    from mas.config import settings

    c = cfg or settings()
    return SandboxSpec(
        docker=c.exec_docker,
        image=c.exec_image,
        cpus=c.exec_cpus,
        memory_mb=c.exec_memory_mb,
        pids=c.exec_pids,
        tmpfs_mb=c.exec_tmpfs_mb,
        max_life_s=c.exec_max_life_s,
    )


def _container_gone(output: str) -> bool:
    o = output.lower()
    return "no such container" in o or "is not running" in o or "container" in o and "not found" in o


class SandboxExecutionBackend:
    """One hardened container per attempt. Started lazily on the first command, removed at close()."""

    name = "sandbox-docker"
    confined = True

    def __init__(self, root: Path, *, attempt_id: Any = None, spec: SandboxSpec | None = None):
        self.root = Path(root).resolve()
        if not self.root.is_dir():
            raise ExecutionError(f"worktree does not exist: {self.root}")
        self.spec = spec or SandboxSpec()
        tag = str(attempt_id).replace("-", "")[:24] if attempt_id else uuid.uuid4().hex[:12]
        self.container = f"mas-exec-{tag}"
        self._started = False
        self._lock = threading.Lock()
        self.commands = 0
        self.image_id: str | None = None  # docker image Id (sha256 of the image config) — pinned identity for the trace
        self.repo_digest: str | None = None  # registry digest, when the image came from one (local builds: None)

    # ------------------------------------------------------------------ container lifecycle

    def _user(self) -> str:
        if self.spec.user:
            return self.spec.user
        if hasattr(os, "getuid"):
            return f"{os.getuid()}:{os.getgid()}"  # type: ignore[attr-defined]
        return "10001:10001"

    def _docker(self, *args: str, timeout: float = 60) -> subprocess.CompletedProcess:
        return subprocess.run([self.spec.docker, *args], capture_output=True, text=True, timeout=timeout, check=False)

    def _ensure(self) -> None:
        with self._lock:
            if self._started:
                return
            self._docker("rm", "-f", self.container, timeout=30)  # a stale one from a crashed predecessor
            s = self.spec
            argv = [
                "run",
                "-d",
                "--name",
                self.container,
                "--init",  # PID 1 that reaps the exec'd processes (otherwise zombies eat the pids limit)
                "--network",
                "none",
                "--read-only",
                "--cap-drop",
                "ALL",
                "--security-opt",
                "no-new-privileges",
                "--pids-limit",
                str(s.pids),
                "--memory",
                f"{s.memory_mb}m",
                "--memory-swap",
                f"{s.memory_mb}m",
                "--cpus",
                str(s.cpus),
                "--user",
                self._user(),
                "--tmpfs",
                f"/tmp:rw,size={s.tmpfs_mb}m",
                "--log-driver",
                "none",
                "--stop-timeout",
                "1",
                "--rm",  # a worker that dies leaves nothing behind once the outer sleep ends
                "-v",
                f"{self.root}:/work:rw",
                "-w",
                "/work",
                "-e",
                "HOME=/tmp",
                "-e",
                "TMPDIR=/tmp",
                "-e",
                "PYTHONDONTWRITEBYTECODE=1",
                "-e",
                "PYTHONUNBUFFERED=1",
                "-e",
                "PYTHONIOENCODING=utf-8",
                "-e",
                "NO_COLOR=1",
                s.image,
                "sleep",
                str(int(s.max_life_s)),
            ]
            try:
                cp = self._docker(*argv, timeout=120)
            except FileNotFoundError as e:
                raise ExecutionUnavailable(f"docker client not found ({s.docker})") from e
            except subprocess.TimeoutExpired as e:
                raise ExecutionError("docker run timed out starting the sandbox") from e
            if cp.returncode != 0:
                raise ExecutionError(f"cannot start sandbox: {cp.stderr.strip()[:500]}")
            self._started = True
            if self.image_id is None:
                self.image_id, self.repo_digest = self._image_identity()

    def _image_identity(self) -> tuple[str | None, str | None]:
        try:
            cp = self._docker("image", "inspect", "--format", '{{.Id}}|{{join .RepoDigests ","}}', self.spec.image, timeout=30)
        except Exception:
            return None, None
        if cp.returncode != 0:
            return None, None
        image_id, _, digests = cp.stdout.strip().partition("|")
        first = digests.split(",")[0].strip() if digests.strip() else None
        return image_id or None, first or None

    def identity(self) -> dict[str, Any]:
        """What ran the commands — recorded in the attempt's execution trace (a mutable tag alone is not evidence)."""
        return {
            "backend": self.name,
            "image": self.spec.image,
            "image_id": self.image_id,
            "repo_digest": self.repo_digest,
            "container": self.container,
            "commands": self.commands,
        }

    def _exec(self, inner: list[str], *, timeout_s: float, cancel: threading.Event | None) -> CommandResult:
        self._ensure()
        t = max(1, int(math.ceil(timeout_s)))
        argv = [self.spec.docker, "exec", self.container, "timeout", "-s", "KILL", str(t), *inner]
        res = run_bounded(
            argv,
            cwd=self.root,
            env=dict(os.environ),  # the docker *client* needs its own env (DOCKER_HOST, ...); nothing of it enters the container
            timeout_s=timeout_s + 3.0,  # the container-side timeout fires first; this is the belt for the client itself
            max_output_bytes=self.spec.max_output_bytes,
            cancel=cancel,
        )
        self.commands += 1
        if res.exit_code == 137 and not res.timed_out:
            res.timed_out = True  # `timeout -s KILL` reports 137 when it killed the command
        if res.exit_code not in (None, 0) and _container_gone(res.output):
            # the sandbox died underneath us (killed / evicted / max_life_s reached): typed error, and the next command
            # gets a fresh container for the same worktree — never a silent "No such container" forever
            with self._lock:
                self._started = False
            res.error = "sandbox container is gone (killed or expired); it will be recreated for the next command"
            res.output = ""
            return res
        if res.cancelled or res.truncated or (res.timed_out and res.exit_code != 137):
            # the client was killed host-side; the process inside may still be running → end everything in the container
            self._kill_all()
        return res

    def _kill_all(self) -> None:
        with self._lock:
            if not self._started:
                return
            self._docker("kill", "--signal", "KILL", self.container, timeout=30)
            self._docker("rm", "-f", self.container, timeout=30)
            self._started = False  # the next command starts a fresh sandbox for the same worktree

    # ------------------------------------------------------------------ ExecutionBackend

    def run(self, argv: list[str], *, timeout_s: float, cancel: threading.Event | None = None) -> CommandResult:
        return self._exec(list(argv), timeout_s=timeout_s, cancel=cancel)

    def run_shell(self, command: str, *, timeout_s: float, cancel: threading.Event | None = None) -> CommandResult:
        return self._exec(["sh", "-c", command], timeout_s=timeout_s, cancel=cancel)

    def close(self) -> None:
        with self._lock:
            try:
                self._docker("rm", "-f", self.container, timeout=30)
            except Exception:
                log.warning("sandbox %s: rm failed", self.container, exc_info=True)
            self._started = False

    def __enter__(self) -> SandboxExecutionBackend:
        return self

    def __exit__(self, *a: Any) -> None:
        self.close()

    def alive(self) -> bool:
        cp = self._docker("ps", "-q", "--filter", f"name=^{self.container}$", timeout=30)
        return bool(cp.stdout.strip())
