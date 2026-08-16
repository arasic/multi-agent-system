"""Per-attempt isolated workspace — roadmap step 6 (git worktrees, ADR-002).

M1 placeholder: no filesystem workspace; agents receive `workspace=None`.
Step 6 will create a git worktree at worktrees/<run>/<task>-<attempt>/ on branch run/<run>/<task>/<attempt>,
mount acceptance/ read-only, and publish commits as git_commit artifacts.
"""

from __future__ import annotations

from pathlib import Path
from uuid import UUID


class Workspace:
    def create(self, run_id: UUID, task_key: str, attempt_number: int) -> Path | None:
        return None

    def cleanup(self, path: Path | None) -> None:
        return None


class NullWorkspace(Workspace):
    pass
