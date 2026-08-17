"""Test tiers — run only what a change can affect, and run it in parallel.

    python scripts/test.py unit            no Postgres, no Docker (validator, providers, tools, agent loop)   ~15 s
    python scripts/test.py core            + Postgres, stub workers, everything LLM-free that needs no Docker ~1 min
    python scripts/test.py full            + Docker (sandboxes, verifier image, service mode)               ~2 min (-n 4)
    python scripts/test.py gate            scripts/stress_step6.py: 100 diamonds / 50 chaos / 20 parallel   ~13 min
    python scripts/test.py area <name>...  the files for an area (see AREAS) — e.g. `area repair planner`
    python scripts/test.py list            print the tiers and areas

Every worker process gets its own `mas_test_<pid>` database (tests/conftest.py), so `-n 4` is safe (MAS_TEST_WORKERS
to change; `auto` = 12 here makes a few timing-sensitive tests flake). Extra pytest arguments pass through
(`-- -k foo -x`). Which tier to run: unit while iterating on pure logic; core before a commit
that touches the orchestrator/planner/workers; full before a commit that touches sandboxes, the verifier, the runner
or service mode; gate after touching leases, the state machine, the worker loop or workspaces (CLAUDE.md).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

TIERS = {
    "unit": ["-m", "not db and not docker"],
    "core": ["-m", "not docker"],
    "full": [],
}

# what to run when you touched ... (files, all relative to tests/)
AREAS: dict[str, list[str]] = {
    "validator": ["test_validator.py", "test_planner_llm.py", "test_repair.py"],
    "planner": ["test_planner_llm.py", "test_questions.py", "test_repair.py", "test_budget_guarantee.py"],
    "repair": ["test_repair.py", "test_budget_guarantee.py", "test_orchestrator.py"],
    "budget": ["test_budget_guarantee.py", "test_telemetry.py", "test_validator.py", "test_orchestrator.py"],
    "state-machine": ["test_state_machine.py", "test_orchestrator.py", "test_repair.py", "test_concurrency.py"],
    "leases": ["test_orchestrator.py", "test_concurrency.py", "test_budget_guarantee.py", "test_orchestrate_service.py"],
    "workspace": ["test_workspace.py", "test_budget_guarantee.py", "test_concurrency.py"],
    "providers": ["test_providers.py", "test_telemetry.py", "test_model_deadlines.py", "test_gateway.py"],
    "tools": ["test_tools.py", "test_llm_agent.py", "test_injection_boundary.py", "test_execution_sandbox.py"],
    "worker": ["test_llm_agent.py", "test_llm_runtime.py", "test_telemetry.py", "test_death_recovery.py"],
    "runner": ["test_exec_runner.py", "test_death_recovery.py", "test_execution_sandbox.py"],
    "verifier": ["test_acceptance.py", "test_adapters.py", "test_orchestrate_service.py"],
    "contracts": ["test_planner_llm.py", "test_adapters.py"],
    "cli": ["test_cli.py", "test_questions.py", "test_evaluation.py"],
    "evaluation": ["test_evaluation.py", "test_cli.py"],
    "conflicts": ["test_conflicts.py", "test_llm_agent.py"],
    "service": ["test_orchestrate_service.py", "test_gateway.py", "test_exec_runner.py"],
}


def _pytest(args: list[str], *, parallel: bool = True) -> int:
    cmd = [sys.executable, "-m", "pytest", "-q"]
    if parallel and _has_xdist():
        # 4, not `auto`: a few tests measure real concurrency / sockets and flake when 12 workers fight for the CPU
        cmd += ["-n", os.environ.get("MAS_TEST_WORKERS", "4")]
    # A short, per-process base avoids Git-for-Windows MAX_PATH failures in nested worktree/ref fixtures and keeps
    # simultaneous test invocations isolated. Callers may still provide their own --basetemp.
    if not any(a == "--basetemp" or a.startswith("--basetemp=") for a in args):
        cmd += [f"--basetemp=.t/p{os.getpid()}"]
    cmd += args
    print("+", " ".join(cmd), flush=True)
    return subprocess.call(cmd, cwd=ROOT)


def _has_xdist() -> bool:
    try:
        import xdist  # noqa: F401
    except ImportError:
        return False
    return True


def main(argv: list[str]) -> int:
    if not argv or argv[0] in {"-h", "--help", "list"}:
        print(__doc__)
        for k, v in AREAS.items():
            print(f"  area {k:14s} {' '.join(v)}")
        return 0
    what, rest = argv[0], argv[1:]
    if rest and rest[0] == "--":
        rest = rest[1:]
    if what in TIERS:
        return _pytest(TIERS[what] + rest)
    if what == "gate":
        return subprocess.call([sys.executable, "scripts/stress_step6.py", *rest], cwd=ROOT)
    if what == "area":
        names = [a for a in rest if a in AREAS]
        extra = [a for a in rest if a not in AREAS]
        if not names:
            print(f"unknown area(s) {rest}; known: {sorted(AREAS)}", file=sys.stderr)
            return 2
        files = sorted({f"tests/{f}" for n in names for f in AREAS[n]})
        return _pytest(files + extra)
    print(f"unknown tier {what!r}; one of {sorted(TIERS)} | gate | area | list", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
