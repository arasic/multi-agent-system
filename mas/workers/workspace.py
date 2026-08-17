"""Per-attempt isolated workspaces (ADR-002, docs/architecture.md §6).

GitWorkspace
    repos/<run_id>.git                        one shared bare repository per run (volume shared by all workers)
    worktrees/<run_id>/<task_uuid>-<attempt>/ one git worktree per attempt, branch run/<run>/<task_uuid>/<attempt>
    (task UUIDs, never planner-controlled keys, in any path or ref)

    create()   worktree from the run's base commit, then *input assembly*: the dependency tasks' git_commit outputs
               are merged in (sequentially, --no-ff) so the worktree contains what the task depends on. A merge
               conflict is left in place and reported to the agent (ctx.conflicts) — resolving it is the agent's job
               (the integration agent's, in particular). Nothing from earlier attempts of the *same* task is inherited.
    publish()  commit whatever the agent left in the worktree; return HEAD if it moved (relative to `start` for
               ordinary tasks, to `base` for integration — its output *is* the assembled merge).
    cleanup()  remove the worktree (branch and commits stay in the bare repo); prune stale worktrees of dead workers.
    promote()  move a run-level ref (e.g. run/<run>/integration) to a sha — used by the verifier stage on PASS.
    show()     read `sha:path` from the object store (how downstream agents read document artifacts).

NullWorkspace: no filesystem; agents get workspace=None (fast unit tests, LLM-free stubs).

Multi-host note: worktrees of a bare repo require a shared filesystem. One host / one volume in the MVP;
cross-host would push/fetch to a remote instead (out of scope, documented).
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID

from mas.models.enums import INTEGRATION_CAPABILITY
from mas.models.types import Artifact, Attempt, Run, Task

log = logging.getLogger(__name__)

EMPTY_TREE = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
GIT_IDENTITY = ["-c", "user.name=mas", "-c", "user.email=mas@localhost", "-c", "commit.gpgsign=false"]
_FIXED_DATE = "2000-01-01T00:00:00Z"  # deterministic base commit sha across racing workers


class WorkspaceError(Exception):
    pass


@dataclass
class WorkspaceHandle:
    path: Path
    repo: Path
    branch: str
    base_sha: str  # run base (before input assembly)
    start_sha: str  # after input assembly (what the agent starts from)
    conflicts: list[str] = field(default_factory=list)  # unresolved paths after input assembly, if any
    merged: list[str] = field(default_factory=list)  # input shas merged in
    meta: dict[str, Any] = field(default_factory=dict)


class Workspace(Protocol):
    name: str

    def create(self, run: Run, task: Task, attempt: Attempt, inputs: list[Artifact]) -> WorkspaceHandle | None: ...
    def publish(self, handle: WorkspaceHandle | None, message: str, *, since: str = "start") -> str | None: ...
    def cleanup(self, handle: WorkspaceHandle | None) -> None: ...

    def gc_run(self, run_id: UUID) -> int: ...
    def promote(self, run_id: UUID, name: str, sha: str) -> None: ...
    def show(self, run_id: UUID, ref: str) -> str: ...


class NullWorkspace:
    name = "none"

    def create(self, run: Run, task: Task, attempt: Attempt, inputs: list[Artifact]) -> WorkspaceHandle | None:
        return None

    def publish(self, handle: WorkspaceHandle | None, message: str, *, since: str = "start") -> str | None:
        return None

    def cleanup(self, handle: WorkspaceHandle | None) -> None:
        return None

    def gc_run(self, run_id: UUID) -> int:
        return 0

    def promote(self, run_id: UUID, name: str, sha: str) -> None:
        return None

    def show(self, run_id: UUID, ref: str) -> str:
        raise WorkspaceError("NullWorkspace has no object store")


def _git(
    *args: str, cwd: Path | None = None, check: bool = True, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    cmd = ["git", *GIT_IDENTITY, *args]
    e = {**os.environ, "GIT_TERMINAL_PROMPT": "0", "GIT_CONFIG_NOSYSTEM": "1", "LC_ALL": "C"}
    if env:
        e.update(env)
    try:
        p = subprocess.run(cmd, cwd=str(cwd) if cwd else None, capture_output=True, text=True, env=e)
    except OSError as ex:  # git missing, bad cwd, permission problems — always a WorkspaceError to callers
        raise WorkspaceError(f"git {' '.join(args)} could not run: {ex}") from ex
    if check and p.returncode != 0:
        raise WorkspaceError(f"git {' '.join(args)} failed ({p.returncode}): {p.stderr.strip() or p.stdout.strip()}")
    return p


def git_available() -> bool:
    return shutil.which("git") is not None


class GitWorkspace:
    name = "git"

    def __init__(self, repo_root: str | Path, worktree_root: str | Path, *, keep_worktrees: bool = False):
        # absolute: git commands run with cwd=<bare repo>, so a relative worktree path would land inside the repo
        self.repo_root = Path(repo_root).resolve()
        self.worktree_root = Path(worktree_root).resolve()
        self.keep_worktrees = keep_worktrees

    # ------------------------------------------------------------------ repo

    def repo_path(self, run_id: UUID) -> Path:
        return self.repo_root / f"{run_id}.git"

    def ensure_repo(self, run_id: UUID, base_ref: str | None = None) -> Path:
        """Idempotent, race-safe: bare repo with a deterministic base commit on `main` (or fetched from base_ref)."""
        repo = self.repo_path(run_id)
        if not (repo / "HEAD").exists():
            try:
                repo.parent.mkdir(parents=True, exist_ok=True)
            except OSError as ex:
                raise WorkspaceError(f"cannot create repo root {repo.parent}: {ex}") from ex
            tmp = repo.with_suffix(f".init-{uuid.uuid4().hex}")  # never PID: every container may be PID 1
            try:
                _git("init", "--bare", "-q", "--initial-branch=main", str(tmp))
                _git("config", "gc.auto", "0", cwd=tmp)
                # Git for Windows otherwise inherits MAX_PATH for ref logs. Attempt branches contain UUIDs and can
                # exceed that limit under pytest or a deeply nested MAS_DATA_DIR even when the worktree itself fits.
                _git("config", "core.longpaths", "true", cwd=tmp)
                if not repo.exists():
                    # Atomic on the same filesystem. A racing loser is harmless, but an unrelated transient Windows
                    # rename error must not be swallowed (doing so deletes the only initialized repo in `finally`).
                    rename_error: OSError | None = None
                    for retry in range(5):
                        try:
                            os.rename(tmp, repo)
                            rename_error = None
                            break
                        except OSError as ex:
                            if repo.exists():  # another initializer won the race with its already-complete temp repo
                                rename_error = None
                                break
                            rename_error = ex
                            time.sleep(0.02 * (retry + 1))
                    if rename_error is not None:
                        msg = f"cannot install initialized bare repository {repo}: {rename_error}"
                        raise WorkspaceError(msg) from rename_error
            finally:
                if tmp.exists():
                    shutil.rmtree(tmp, ignore_errors=True)
        # A racing initializer can observe the destination immediately after rename while Windows is still making it
        # available as a process working directory. Wait for the repository marker, then configure it from its stable
        # parent instead of using the just-renamed directory as cwd (CreateProcess may otherwise raise WinError 267).
        deadline = time.monotonic() + 2.0
        while not (repo.is_dir() and (repo / "HEAD").is_file()) and time.monotonic() < deadline:
            time.sleep(0.02)
        if not (repo.is_dir() and (repo / "HEAD").is_file()):
            raise WorkspaceError(f"bare repository initialization did not complete: {repo}")

        # Base commit on main (deterministic sha, so racing creators agree). --git-dir lets this also upgrade
        # repositories created by older releases without depending on the repository itself being a valid cwd.
        _git("--git-dir", str(repo), "config", "core.longpaths", "true", cwd=repo.parent, check=False)
        head = _git("rev-parse", "-q", "--verify", "refs/heads/main", cwd=repo, check=False)
        if head.returncode != 0:
            if base_ref:
                _git("fetch", "-q", base_ref, "HEAD", cwd=repo)
                sha = _git("rev-parse", "FETCH_HEAD", cwd=repo).stdout.strip()
            else:
                sha = _git(
                    "commit-tree",
                    EMPTY_TREE,
                    "-m",
                    "mas base",
                    cwd=repo,
                    env={"GIT_AUTHOR_DATE": _FIXED_DATE, "GIT_COMMITTER_DATE": _FIXED_DATE},
                ).stdout.strip()
            # create only if still absent (old value = zeros); a racing loser is harmless
            _git("update-ref", "refs/heads/main", sha, "0" * 40, cwd=repo, check=False)
        _git("worktree", "prune", cwd=repo, check=False)  # entries left by dead workers
        return repo

    def base_sha(self, run_id: UUID) -> str:
        return _git("rev-parse", "refs/heads/main", cwd=self.repo_path(run_id)).stdout.strip()

    # ------------------------------------------------------------------ per attempt

    def worktree_path(self, run_id: UUID, task_id: UUID, attempt_number: int) -> Path:
        # task UUID, never the planner-controlled task key (no path traversal, no odd characters)
        return self.worktree_root / str(run_id) / f"{task_id}-{attempt_number}"

    @staticmethod
    def branch_name(run_id: UUID, task_id: UUID, attempt_number: int) -> str:
        return f"run/{run_id}/{task_id}/{attempt_number}"

    def create(self, run: Run, task: Task, attempt: Attempt, inputs: list[Artifact]) -> WorkspaceHandle:
        try:
            repo = self.ensure_repo(run.id, run.base_ref)
            base = self.base_sha(run.id)
            path = self.worktree_path(run.id, task.id, attempt.attempt_number)
            branch = self.branch_name(run.id, task.id, attempt.attempt_number)
            if path.exists():  # a previous incarnation of this attempt (dead worker); start clean
                _git("worktree", "remove", "--force", str(path), cwd=repo, check=False)
                shutil.rmtree(path, ignore_errors=True)
            path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as ex:
            raise WorkspaceError(f"cannot prepare workspace: {ex}") from ex
        # branch may exist if a dead worker created it before dying; reset it to base
        _git("branch", "-f", branch, base, cwd=repo, check=False)
        _git("worktree", "add", "-q", "-f", str(path), branch, cwd=repo)
        handle = WorkspaceHandle(path=path, repo=repo, branch=branch, base_sha=base, start_sha=base, meta={"task_key": task.key})
        # input assembly: merge dependency git_commit outputs, in order; leave conflicts for the agent
        shas: list[str] = []
        for a in inputs:
            if a.type == "git_commit" and a.ref not in shas:
                shas.append(a.ref)
        for sha in shas:
            m = _git("merge", "--no-ff", "--no-edit", "-q", sha, cwd=path, check=False)
            if m.returncode != 0:
                conflicted = _git("diff", "--name-only", "--diff-filter=U", cwd=path, check=False).stdout.split()
                if conflicted:
                    handle.conflicts = conflicted
                    handle.meta["conflict_sha"] = sha
                    log.warning(
                        "input assembly conflict for %s#%s on %s: %s", task.key, attempt.attempt_number, sha[:8], conflicted
                    )
                    break  # leave the conflicted state in place for the agent
                raise WorkspaceError(f"input assembly failed merging {sha[:8]}: {m.stderr.strip()}")
            handle.merged.append(sha)
        handle.start_sha = _git("rev-parse", "HEAD", cwd=path).stdout.strip()
        return handle

    def publish(self, handle: WorkspaceHandle | None, message: str, *, since: str = "start") -> str | None:
        """Stage + commit whatever the agent left; return HEAD if it moved relative to `since` ('start'|'base')."""
        if handle is None:
            return None
        if handle.conflicts:
            # unresolved conflicts still present? then there is nothing valid to publish
            still = _git("diff", "--name-only", "--diff-filter=U", cwd=handle.path, check=False).stdout.split()
            if still:
                return None
        _git("add", "-A", cwd=handle.path)
        dirty = _git("diff", "--cached", "--quiet", cwd=handle.path, check=False).returncode != 0
        if dirty:
            _git("commit", "-q", "-m", message, cwd=handle.path)
        head = _git("rev-parse", "HEAD", cwd=handle.path).stdout.strip()
        ref = handle.base_sha if since == "base" else handle.start_sha
        return head if head != ref else None

    def cleanup(self, handle: WorkspaceHandle | None) -> None:
        if handle is None or self.keep_worktrees:
            return
        try:
            _git("worktree", "remove", "--force", str(handle.path), cwd=handle.repo, check=False)
        except WorkspaceError:
            log.debug("worktree remove failed; falling back to rmtree", exc_info=True)
        shutil.rmtree(handle.path, ignore_errors=True)
        # NOT the per-run directory: a sibling attempt of the same run may be creating its worktree right now (removing
        # the parent underneath `git worktree add` failed 3/170 runs in the stress gate). gc_run removes it at run end.

    def gc_run(self, run_id: UUID) -> int:
        """Remove every worktree directory of a run — called by the orchestrator once the run is terminal, so what a
        crashed or abandoned worker left behind on purpose (its worktree is evidence while the run is live) does not
        leak. Returns how many attempt worktrees were removed. Commits and branches stay in the bare repo."""
        if self.keep_worktrees:
            return 0
        run_dir = self.worktree_root / str(run_id)
        if not run_dir.exists():
            return 0
        repo = self.repo_path(run_id)
        has_repo = (repo / "HEAD").exists()
        removed = 0
        for child in sorted(run_dir.iterdir()):
            if not child.is_dir():
                continue
            if has_repo:
                try:
                    _git("worktree", "remove", "--force", str(child), cwd=repo, check=False)
                except WorkspaceError:
                    log.debug("worktree remove failed during gc; falling back to rmtree", exc_info=True)
            shutil.rmtree(child, ignore_errors=True)
            removed += 1
        if has_repo:
            try:
                _git("worktree", "prune", cwd=repo, check=False)
            except WorkspaceError:
                log.debug("worktree prune failed during gc", exc_info=True)
        shutil.rmtree(run_dir, ignore_errors=True)
        return removed

    # ------------------------------------------------------------------ run-level

    def promote(self, run_id: UUID, name: str, sha: str) -> None:
        repo = self.repo_path(run_id)
        if not (repo / "HEAD").exists():
            return
        _git("update-ref", f"refs/heads/run/{run_id}/{name}", sha, cwd=repo)

    def show(self, run_id: UUID, ref: str) -> str:
        return _git("show", ref, cwd=self.repo_path(run_id)).stdout

    def tree_sha(self, run_id: UUID, sha: str) -> str | None:
        """The tree hash behind a commit — what was actually verified (progress fingerprint, step 13-lite): a repair that
        produced an identical tree repeats it even though the commit id differs."""
        r = _git("rev-parse", "-q", "--verify", f"{sha}^{{tree}}", cwd=self.repo_path(run_id), check=False)
        return r.stdout.strip() or None

    def files_at(self, run_id: UUID, sha: str) -> list[str]:
        out = _git("ls-tree", "-r", "--name-only", sha, cwd=self.repo_path(run_id)).stdout
        return [ln for ln in out.splitlines() if ln]

    def is_ancestor(self, run_id: UUID, ancestor: str, sha: str) -> bool:
        return _git("merge-base", "--is-ancestor", ancestor, sha, cwd=self.repo_path(run_id), check=False).returncode == 0


def since_for(task: Task) -> str:
    """Integration's output is the assembled merge itself; other tasks must add something on top of their inputs."""
    return "base" if task.capability == INTEGRATION_CAPABILITY else "start"


def workspace_from_settings() -> Workspace:
    from mas.config import settings

    s = settings()
    if s.workspace == "git" and git_available():
        return GitWorkspace(s.repo_root, s.worktree_root, keep_worktrees=s.keep_worktrees)
    return NullWorkspace()
