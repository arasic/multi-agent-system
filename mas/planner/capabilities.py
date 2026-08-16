"""Capability → tool allow-list registry (validator rule 4; invariants I-7, I-8, I-11; antipatterns A2/A8/B12).

A task may only request tools its capability allows; the validator fills the default when a task requests none.
The tool implementations themselves arrive with the LLM worker (roadmap step 10); this is the *policy* half —
deterministic, and enforced before any agent runs.
"""

from __future__ import annotations

from mas.models.enums import INTEGRATION_CAPABILITY, SOLVE_CAPABILITY

# Tool names are opaque identifiers here; the worker's tool layer binds them to implementations.
TOOL_FILESYSTEM = "filesystem"  # read/write inside the attempt's worktree only
TOOL_PYTHON = "python"  # run python / tests inside the worktree
TOOL_SHELL = "shell"  # bounded shell inside the worktree
TOOL_GIT = "git"  # commit/branch inside the worktree; never push, never touch main
TOOL_MODEL = "model"  # call the assigned ModelProvider

KNOWN_TOOLS: frozenset[str] = frozenset({TOOL_FILESYSTEM, TOOL_PYTHON, TOOL_SHELL, TOOL_GIT, TOOL_MODEL})

DEFAULT_CAPABILITY_TOOLS: dict[str, frozenset[str]] = {
    "architecture": frozenset({TOOL_FILESYSTEM, TOOL_MODEL}),
    "implementation": frozenset({TOOL_FILESYSTEM, TOOL_PYTHON, TOOL_SHELL, TOOL_GIT, TOOL_MODEL}),
    "testing": frozenset({TOOL_FILESYSTEM, TOOL_PYTHON, TOOL_SHELL, TOOL_GIT, TOOL_MODEL}),
    INTEGRATION_CAPABILITY: frozenset({TOOL_FILESYSTEM, TOOL_PYTHON, TOOL_SHELL, TOOL_GIT, TOOL_MODEL}),
    SOLVE_CAPABILITY: frozenset(KNOWN_TOOLS),  # single-agent baseline: everything the MAS collectively gets
}

# Never grantable in the MVP regardless of capability (I-11): anything with external effect.
FORBIDDEN_TOOLS: frozenset[str] = frozenset({"network", "deploy", "git_push", "acceptance_write"})


def allowed_tools(capability: str, registry: dict[str, frozenset[str]] | None = None) -> frozenset[str]:
    reg = registry if registry is not None else DEFAULT_CAPABILITY_TOOLS
    return reg.get(capability, frozenset())
