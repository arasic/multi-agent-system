"""Run the frozen M3 A/B/C/D x N matrix and write machine-readable evidence plus a compact SVG report.

Real evaluation (minimum five repetitions per cell):
  python scripts/benchmark.py --cheap-model openai:... --strong-model anthropic:... \
      --planner-model anthropic:... --worker-model openai:... --repeats 5

Key-less substrate rehearsal (hand-written width DAG, stub agents, real verifier):
  python scripts/benchmark.py --offline --repeats 1

Every cell uses the same goal, acceptance suite and total budgets. A/B are produced by the runtime's single-agent
policy. JSONL is appended after each run so an interrupted experiment remains auditable. Unpriced calls make cost
null in the summary rather than falsely cheap.

Experimental design (ADR-009):
  * **Paired C/D.** Each (N, repetition) block gets exactly ONE validated plan — `mas plan` with the live planner,
    under the parallel budget (`max_concurrency = N`) — recorded with its SHA-256 and replayed by both C and D
    (`mas run --dag`). C and D therefore differ in concurrency alone; planning cost is measured once per block and
    reported separately, not counted inside either configuration. (Offline, the block's "plan" is the hand-written
    width fixture, so the same plumbing is rehearsed without a model.)
  * **Randomized block schedule.** Provider load, rate limits, cache warmth and time of day drift over a matrix that
    runs for hours. The order of blocks, and of A/B/C/D inside each block, is drawn from a seeded PRNG; the seed and
    the resulting schedule are frozen in `experiment.json` and replayed verbatim on resume.
  * **Environment identity.** The manifest freezes not only models, prices, budgets and suite hashes but everything
    else that changes behavior: Python/platform, provider timeout/retries, the Anthropic request-shape settings, the
    per-attempt call budget, and the sandbox/verifier image ids and limits. A different environment is a different
    experiment and will not resume into the same directory.

Protocol and safety (ADR-010):
  * **Equal *total* budgets across the plan boundary.** A and B plan inside their run, so their planning is already
    inside their budget. C and D plan once per block, outside the run — so their execution runs get only what the
    block's shared plan left (`execution_budgets`), and each row also carries the system-level totals that add the
    planning back (`system_call_cost_usd`, `system_machine_s`) for comparison against A/B. C versus D stays an
    execution-only contrast: the shared planning component is identical and cancels.
  * **An aggregate spend ceiling.** `--max-total-cost-usd` bounds the whole matrix, not just each run. Spend is
    recomputed from the raw append-only logs (superseded and retried operations included — the provider billed those
    too); before every operation the harness requires `spent + this operation's ceiling <= cap`, prints the billed /
    remaining / worst-case projection, and stops if unpriced usage makes the total unknowable. The ceiling is recorded
    in `experiment.json` (with an append-only history of any change) and audited by `scripts/mvp_gate.py`.
  * **Pacing and a circuit breaker.** `--pace-s` between operations, `--cooldown-s` after a machinery failure, and a
    stop after `--max-consecutive-infrastructure` failures in a row: a provider-wide incident must never be walked
    through automatically. Stopping is always resumable — rerun the same command.
  * **No clarification answer key (unattended M3).** The width benchmarks are frozen and fully specified, and the
    matrix runs unattended; a planner that asks a question is a valid *experimental* planning outcome, not an
    infrastructure failure. Clarification is exercised deliberately in `scripts/live_smoke.py` (ADR-006).

Failure classification (`classify_run`): a run that ends PASSED, or FAILED/ABORTED because the model, the plan or the
experiment's budgets did (`experimental`), is evidence and stays. A run that ends because the machinery did — verifier
crash/timeout, unusable suite, provider outage, sandbox/workspace failure, worker death, a client that could not read
the run — is `infrastructure`: it cannot answer the MAS value question, it is kept in the append-only log for audit,
and its cell is rerun on the next invocation. Completion counts only the last row per cell/repetition, and only if
every earlier row for that key was infrastructure-invalid (a valid row is never rerun; two valid rows for one key are
a duplicate).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import platform
import random
import re
import statistics
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from mas.config import settings  # noqa: E402
from mas.db import connect  # noqa: E402
from mas.evaluation import CONFIGS, WIDTHS, width_dag, width_goal  # noqa: E402
from mas.metrics import compute  # noqa: E402
from mas.planner.dag import plan_digest  # noqa: E402

RUN_ID = re.compile(r"\brun ([0-9a-f-]{36})\b", re.I)
TERMINAL = {"PASSED", "FAILED", "ABORTED"}
FAILURE_CLASSES = ("pass", "experimental", "infrastructure")
REPLAY_CONFIGS = ("C", "D")  # the configurations that execute the block's shared plan (ADR-009)
# a plan-only run that ended without a DAG: which of those outcomes is the system under test, and which is machinery
_EXPERIMENTAL_PLAN_REASONS = ("INVALID_PLAN", "UNSUPPORTED", "NO_PROGRESS", "POLICY_DENIED", "BUDGET_EXHAUSTED")
# verdict texts (state_machine.fail_run) that name the machinery rather than the code under test
_INFRASTRUCTURE_VERDICT_MARKERS = (
    "verification not completed",  # verifier ERROR/TIMEOUT twice, or INVALID
    "repair needs a planner and none is configured",  # operator/config error, not the model
)
# per-run metrics reported as distributions per (config, N) cell — evaluation.md §4
DISTRIBUTION_FIELDS = (
    ("machine_s", "machine seconds (total minus human wait)"),
    ("wall_clock_s", "execution wall-clock seconds"),
    ("human_wait_s", "human wait seconds (clarifying questions)"),
    ("critical_path_s", "critical-path seconds (longest dependency chain of task work)"),
    ("parallelism_efficiency", "parallelism efficiency (sum of attempt seconds / wall-clock)"),
    ("worker_utilisation", "worker utilisation (attempt seconds / (wall-clock x workers))"),
    ("call_cost_usd", "cost USD (metered model calls)"),
    ("system_call_cost_usd", "system-level cost USD (run + the block's shared planning, ADR-010)"),
    ("system_machine_s", "system-level machine seconds (run + the block's shared planning, ADR-010)"),
    ("call_input_tokens", "input tokens"),
    ("call_output_tokens", "output tokens"),
    ("call_cache_read_tokens", "cache-read tokens"),
    ("tokens_in_per_attempt", "input tokens per attempt (context scoping)"),
    ("model_calls", "model calls"),
    ("call_latency_s", "mean model-call latency seconds"),
    ("tasks", "tasks created"),
    ("attempts", "attempts"),
    ("retries", "retries"),
    ("abandoned", "abandoned attempts (worker death)"),
    ("replans_used", "re-plans (bounded repair cycles)"),
    ("plan_rejections", "planner rounds rejected by the validator"),
    ("questions", "clarifying-question batches"),
    ("assumptions", "assumptions recorded by the planner"),
    ("verifier_fails", "acceptance failures (verifier FAIL verdicts)"),
)
_PERCENTILES = ((0.0, "min"), (0.25, "p25"), (0.5, "median"), (0.75, "p75"), (1.0, "max"))


def classify_run(row: dict) -> str:
    """`pass` | `experimental` | `infrastructure` — deterministic, from the recorded status/verdict/attempt classes.

    Experimental: the model, the plan or the experiment's budgets decided the outcome (verifier FAIL exhausting the repair
    budget, NO_PROGRESS, token/cost/wall-clock abort, invalid or unmappable plan, policy denial, an agent that could not
    do the task). Infrastructure: the run never got a fair chance (verifier crash/timeout/unusable suite, missing
    planner, a task whose every failed attempt was a workspace/sandbox/provider/agent-crash failure or a worker death,
    a client that lost the run).

    `UNSUPPORTED` is not infrastructure by itself: the planner earns it too (a contract that maps to no trusted adapter,
    ADR-008 §6), which is the model's outcome. Only the verifier's INVALID — "verification not completed" — is.
    `CANCELLED` (ADR-009) always is: an operator ended that run, so it says nothing about MAS.
    """
    status = row.get("status")
    if row.get("plan_failed"):
        # C/D never started: the block's shared plan could not be produced (ADR-009). The planner is part of the
        # system under test, so its refusals/invalid plans/budget are evidence; a crash or a lost client is not.
        if row.get("plan_outcome") == "questions":
            # ADR-010: the M3 benchmarks are frozen, fully specified and run unattended, so there is deliberately no
            # answer key. Choosing to ask rather than to plan is the planner's own outcome, hence evidence.
            return "experimental"
        return "experimental" if row.get("verdict_reason") in _EXPERIMENTAL_PLAN_REASONS else "infrastructure"
    if status == "PASSED":
        return "pass"
    if status not in TERMINAL:
        return "infrastructure"  # CLIENT_ERROR, or a run the client left non-terminal
    verdict = str(row.get("verdict") or "")
    reason = row.get("verdict_reason")
    if status == "ABORTED":
        # budgets are the experiment's parameters; anything else that aborts a run is not the model's doing
        return "experimental" if reason in (None, "BUDGET_EXHAUSTED") else "infrastructure"
    if any(marker in verdict for marker in _INFRASTRUCTURE_VERDICT_MARKERS):
        return "infrastructure"
    if reason == "UNRECOVERABLE_FAILURE":
        # a task exhausted its attempts: the model's failure unless *every* failed attempt was the machinery's — a
        # worker death (`abandoned`) or a cancellation is never the model's doing, so attempts that only ever ended
        # that way exonerate it; a run without recorded classes stays evidence (nothing exonerates the model)
        classes = row.get("attempt_failure_classes") if isinstance(row.get("attempt_failure_classes"), dict) else {}
        counted = {k: int(v) for k, v in classes.items() if int(v) > 0}
        blamed = {k for k in counted if k not in ("abandoned", "cancelled")}
        if counted and blamed <= {"infrastructure"}:
            return "infrastructure"
        return "experimental"
    return "experimental"  # NO_PROGRESS, BUDGET_EXHAUSTED, INVALID_PLAN, POLICY_DENIED, UNSUPPORTED (planner)


def row_key(row: dict) -> tuple:
    return (row.get("config"), row.get("n"), row.get("repetition"))


def effective_rows(rows: list[dict]) -> tuple[dict[tuple, dict], dict[str, Any]]:
    """Last row per key wins, provided every earlier row for that key was infrastructure-invalid.

    Returns (effective row per key, audit) where audit has `superseded` (infrastructure rows replaced by a rerun) and
    `duplicates` (keys where a valid row was followed by another row — never legitimate)."""
    by_key: dict[tuple, list[dict]] = {}
    for row in rows:
        by_key.setdefault(row_key(row), []).append(row)
    effective: dict[tuple, dict] = {}
    duplicates: list[tuple] = []
    superseded = 0
    for key, seq in by_key.items():
        for earlier in seq[:-1]:
            if classify_run(earlier) == "infrastructure":
                superseded += 1
            else:
                duplicates.append(key)
                break
        effective[key] = seq[-1]
    return effective, {"superseded": superseded, "duplicates": sorted(set(duplicates), key=str)}


def _git_state() -> tuple[str, bool]:
    rev = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=False)
    dirty = subprocess.run(["git", "status", "--porcelain"], cwd=ROOT, capture_output=True, text=True, check=False)
    return (rev.stdout.strip() if rev.returncode == 0 else "unknown", bool(dirty.stdout.strip()) or dirty.returncode != 0)


def _suite_sha(n: int) -> str:
    """Identity of the complete trusted suite directory, including filenames and bytes."""
    root = ROOT / "acceptance" / f"adapters_{n}"
    h = hashlib.sha256()
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        h.update(path.relative_to(root).as_posix().encode())
        h.update(b"\0")
        h.update(path.read_bytes())
        h.update(b"\0")
    return h.hexdigest()


def _image_id(image: str) -> str | None:
    """The exact local image the sandboxes/verifier would use, not just its tag (a rebuilt `:latest` is a new one)."""
    p = subprocess.run(
        [settings().exec_docker, "image", "inspect", "--format", "{{.Id}}", image],
        capture_output=True,
        text=True,
        check=False,
    )
    return p.stdout.strip() or None


def environment_spec() -> dict:
    """Everything outside models/prices/budgets that changes how a run behaves (reviewer finding, 2026-08-17).

    Frozen with the experiment: resuming in a different environment — another Python, a rebuilt sandbox image, a
    different provider timeout or Anthropic request shape — is a different experiment, not a continuation."""
    s = settings()
    return {
        "python": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "provider": {
            "timeout_s": s.provider_timeout_s,
            "max_retries": s.provider_max_retries,
            "anthropic_thinking": s.anthropic_thinking,
            "anthropic_effort": s.anthropic_effort,
            "anthropic_fallbacks": s.anthropic_fallbacks,
            "openai_base_url": s.openai_base_url,
            "openai_max_tokens_field": s.openai_max_tokens_field,
        },
        "attempt": {"max_calls": s.attempt_max_calls, "max_tokens": s.attempt_max_tokens},
        "exec": {
            "backend": s.exec_backend,
            "image": s.exec_image,
            "image_id": _image_id(s.exec_image),
            "cpus": s.exec_cpus,
            "memory_mb": s.exec_memory_mb,
            "pids": s.exec_pids,
            "tmpfs_mb": s.exec_tmpfs_mb,
            "max_life_s": s.exec_max_life_s,
        },
        "verifier": {
            "image": s.verifier_image,
            "image_id": _image_id(s.verifier_image),
            "timeout_s": s.verifier_timeout_s,
            "cpus": s.verifier_cpus,
            "memory_mb": s.verifier_memory_mb,
            "pids": s.verifier_pids,
        },
        "worker_capabilities": list(s.worker_capabilities),
    }


def build_schedule(configs: list[str], widths: list[int], repeats: int, seed: int) -> list[dict]:
    """Deterministic randomized block schedule (ADR-009): the (N, repetition) blocks in drawn order, and A/B/C/D in a
    drawn order inside each block, so provider/time drift cannot line up with a configuration."""
    rng = random.Random(seed)
    blocks = [{"n": n, "repetition": r} for n in widths for r in range(1, repeats + 1)]
    rng.shuffle(blocks)
    schedule = []
    for index, block in enumerate(blocks, 1):
        order = list(configs)
        rng.shuffle(order)
        schedule.append({"index": index, "n": block["n"], "repetition": block["repetition"], "configs": order})
    return schedule


def schedule_cells(schedule: list[dict]) -> list[tuple]:
    return [(c, int(b["n"]), int(b["repetition"])) for b in schedule for c in b["configs"]]


def experiment_spec(args) -> dict:
    """Everything that must remain frozen across a resumable experiment."""
    prices = settings().model_prices.strip()
    try:
        price_snapshot = json.loads(prices) if prices else {}
    except json.JSONDecodeError:
        price_snapshot = prices
    return {
        "schema": 3,
        "mode": "offline" if args.offline else "live",
        "configs": list(args.configs),
        "widths": list(args.widths),
        "repeats": args.repeats,
        "seed": args.seed,
        "models": {
            "cheap": args.cheap_model,
            "strong": args.strong_model,
            "planner": args.planner_model,
            "worker": args.worker_model,
        },
        "budgets": {
            "max_tokens": args.max_tokens,
            "max_attempt_tokens": args.max_attempt_tokens,
            "max_cost_usd": args.max_cost_usd,
            "max_wallclock_s": args.max_wallclock_s,
            "max_replans": args.max_replans,
        },
        "model_prices": price_snapshot,
        "suite_sha256": {str(n): _suite_sha(n) for n in args.widths},
        "environment": environment_spec(),
    }


def open_experiment(output: Path, spec: dict, *, spend_cap_usd: float | None = None) -> dict:
    """Create or resume one immutable experiment. Refuse to mix configurations in the same evidence directory.

    The aggregate spend ceiling (ADR-010) is recorded in the manifest but *outside* `spec`: freezing it inside the
    experiment's identity would make an experiment that reaches its ceiling unresumable, so the operator's only way
    forward would be to discard the evidence already paid for. It is instead append-only history — every change is
    stamped with when, at which commit, and from what to what, and the gate audits the recomputed spend against the
    recorded ceiling."""
    path = output / "experiment.json"
    revision, dirty = _git_state()
    schedule = build_schedule(list(spec["configs"]), list(spec["widths"]), int(spec["repeats"]), int(spec["seed"]))
    now = datetime.now(UTC).isoformat()
    if path.exists():
        manifest = json.loads(path.read_text(encoding="utf-8"))
        if manifest.get("spec") != spec:
            raise ValueError(f"{path} belongs to a different experiment; choose a new --output directory")
        if manifest.get("git_commit") != revision:
            raise ValueError(f"{path} was started at commit {manifest.get('git_commit')}; current commit is {revision}")
        if manifest.get("git_dirty") is False and dirty:
            raise ValueError("the experiment started from a clean tree but the current tree has uncommitted changes")
        recorded = manifest.get("schedule")
        if not isinstance(recorded, list) or sorted(schedule_cells(recorded)) != sorted(schedule_cells(schedule)):
            raise ValueError(f"{path} records a schedule that no longer covers this matrix; choose a new --output directory")
        if spend_cap_usd is not None and manifest.get("spend_cap_usd") != spend_cap_usd:
            previous = manifest.get("spend_cap_usd")
            manifest["spend_cap_usd"] = spend_cap_usd
            manifest.setdefault("spend_cap_history", []).append(
                {"at": now, "git_commit": revision, "from_usd": previous, "to_usd": spend_cap_usd}
            )
            print(f"[spend] ceiling changed from {previous} to {spend_cap_usd} USD (recorded in experiment.json)", flush=True)
            path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return manifest  # the recorded order is the experiment's design: replay it, never redraw it
    manifest = {
        "schema": 3,
        "experiment_id": str(uuid4()),
        "created_at": now,
        "git_commit": revision,
        "git_dirty": dirty,
        "spec": spec,
        "schedule": schedule,
        "spend_cap_usd": spend_cap_usd,
        "spend_cap_history": [{"at": now, "git_commit": revision, "from_usd": None, "to_usd": spend_cap_usd}],
    }
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


def load_rows(path: Path, experiment_id: str) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSONL at {path}:{number}: {exc}") from exc
        if row.get("experiment_id") != experiment_id:
            raise ValueError(f"foreign experiment row at {path}:{number}")
        rows.append(row)
    return rows


def load_plans(path: Path, experiment_id: str) -> dict[tuple[int, int], dict]:
    """The block plans recorded so far, keyed by (N, repetition). Last record for a block wins (a failed plan may be
    retried by a later invocation); every record stays in the append-only file."""
    plans: dict[tuple[int, int], dict] = {}
    for record in load_rows(path, experiment_id):
        plans[(int(record["n"]), int(record["repetition"]))] = record
    return plans


def plan_command(args, n: int) -> list[str]:
    """`mas plan`: one planning round with the live planner, under the *parallel* budget (ADR-009 §2)."""
    return [
        sys.executable,
        "-m",
        "mas",
        "plan",
        "--goal",
        width_goal(n),
        "--benchmark",
        f"adapters_{n}",
        "--planner",
        "llm",
        "--planner-model",
        args.planner_model,
        "--max-concurrency",
        str(n),
        "--max-tokens",
        str(args.max_tokens),
        "--max-attempt-tokens",
        str(args.max_attempt_tokens),
        "--max-cost-usd",
        str(args.max_cost_usd),
        "--max-wallclock-s",
        str(args.max_wallclock_s),
        "--max-replans",
        str(args.max_replans),
        "--json",
    ]


def make_plan(args, n: int, repetition: int, path: Path) -> dict:
    """Produce the one plan configs C and D of this block will both execute, and record what it cost.

    Offline the block's plan is the hand-written width fixture (no model, same plumbing); live it is a real metered
    planning round through `mas plan`. Never raises: a plan that could not be produced is recorded as such and the
    block's C/D rows say so."""
    record: dict[str, Any] = {"n": n, "repetition": repetition, "path": path.name}
    if args.offline:
        dag = width_dag(n).to_dict()
        path.write_text(json.dumps(dag, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        record.update(
            {
                "source": "fixture",
                "planned": True,
                "outcome": "planned",
                "plan_sha256": plan_digest(dag),
                "tasks": len(dag["tasks"]),
                "dag": dag,
                "cost_known": True,
                "call_cost_usd": 0.0,
                "model_calls": 0,
                "plan_s": 0.0,
            }
        )
        return record
    argv = plan_command(args, n) + ["--output", str(path)]
    print(f"\n[plan N={n} repeat={repetition}] " + " ".join(argv[3:]), flush=True)
    proc = subprocess.run(argv, cwd=ROOT, text=True, capture_output=True, check=False)
    print(proc.stdout, end="")
    if proc.stderr:
        print(proc.stderr, file=sys.stderr, end="")
    record["source"] = "planner"
    try:
        start = proc.stdout.index("{")
        reported = json.loads(proc.stdout[start:])
    except (ValueError, json.JSONDecodeError):
        record.update({"planned": False, "outcome": "client_error", "error": "no JSON record from `mas plan`"})
        return record
    record.update({k: v for k, v in reported.items() if k != "output"})
    if not reported.get("planned"):
        record.update({"planned": False, "outcome": "questions" if reported.get("parked") else "failed"})
        return record
    try:
        dag = json.loads(path.read_text(encoding="utf-8"))
        digest = plan_digest(dag)
    except (OSError, json.JSONDecodeError) as exc:
        record.update({"planned": False, "outcome": "client_error", "error": f"cannot read the exported plan: {exc}"})
        return record
    if digest != reported.get("plan_sha256"):  # the file must be the plan the run recorded
        record.update({"planned": False, "outcome": "client_error", "error": f"exported plan digest {digest} != reported"})
        return record
    record.update({"planned": True, "outcome": "planned", "dag": dag, "tasks": len(dag.get("tasks", []))})
    return record


def block_plan(args, plans: dict[tuple[int, int], dict], n: int, repetition: int, plans_dir: Path) -> dict:
    """The block's plan, produced once and reused: a recorded plan is evidence and is never redrawn, only restored to
    disk if the file went missing. A recorded *failure* is retried on the next invocation, like an invalid cell."""
    path = plans_dir / f"n{n:02d}-r{repetition}.json"
    previous = plans.get((n, repetition))
    if previous is not None and previous.get("planned"):
        dag = previous.get("dag")
        try:  # the file is a convenience; plans.jsonl is the evidence, so a lost or damaged file is just rewritten
            on_disk = plan_digest(json.loads(path.read_text(encoding="utf-8"))) if path.exists() else None
        except (OSError, json.JSONDecodeError):
            on_disk = None
        if isinstance(dag, dict) and on_disk != previous.get("plan_sha256"):
            path.write_text(json.dumps(dag, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"[resume] plan N={n} repeat={repetition} already recorded ({str(previous.get('plan_sha256'))[:12]})", flush=True)
        return previous
    return make_plan(args, n, repetition, path)


def plan_failure_row(config: str, n: int, repetition: int, plan: dict) -> dict:
    """C or D could not run: there is no shared plan for this block. Recorded as a row so the cell is not silently
    missing — `classify_run` decides from the planning run's own reason whether that is evidence or machinery."""
    return {
        "config": config,
        "n": n,
        "repetition": repetition,
        "status": plan.get("status") or "PLAN_FAILED",
        "verdict": plan.get("verdict") or plan.get("error") or "the block's shared plan could not be produced",
        "verdict_reason": plan.get("verdict_reason"),
        "plan_failed": True,
        "plan_outcome": plan.get("outcome"),
        "plan_run_id": plan.get("run_id"),
        "cost_known": bool(plan.get("cost_known", False)),
        "call_cost_usd": plan.get("call_cost_usd"),
    }


def plan_usage(plan: dict | None) -> dict:
    """What a block's shared planning round already spent of that block's equal total budget."""
    plan = plan or {}
    cost = plan.get("call_cost_usd")
    known = bool(plan.get("cost_known")) and cost is not None
    return {
        "tokens": int(plan.get("call_input_tokens") or 0) + int(plan.get("call_output_tokens") or 0),
        "cost_usd": float(cost) if cost is not None else None,
        "seconds": float(plan.get("plan_s") or 0.0),
        "cost_known": known,
    }


def full_budgets(args) -> dict:
    """The run budget every configuration starts from (evaluation.md §3: equal totals)."""
    return {
        "max_tokens": int(args.max_tokens),
        "max_cost_usd": float(args.max_cost_usd),
        "max_wallclock_s": int(args.max_wallclock_s),
    }


def execution_budgets(args, plan: dict | None) -> dict:
    """C/D execute under what the block's shared plan left of the run's *total* budget (ADR-010).

    A and B plan inside their run, so their planning is already charged against their tokens, cost and wall-clock. C
    and D plan once per block, outside the run: if execution then received the full budget again, they would silently
    get more than A/B and the comparison would stop being fair. Unpriced planning subtracts nothing — that case stops
    the matrix instead (`spend_ledger`), rather than quietly guessing."""
    if plan is None:
        return full_budgets(args)
    usage = plan_usage(plan)
    spent = usage["cost_usd"] if usage["cost_known"] else 0.0
    return {
        "max_tokens": max(0, int(args.max_tokens) - usage["tokens"]),
        "max_cost_usd": round(max(0.0, float(args.max_cost_usd) - (spent or 0.0)), 6),
        "max_wallclock_s": max(0, int(args.max_wallclock_s) - int(usage["seconds"])),
    }


def attach_system_totals(row: dict, plan: dict | None) -> dict:
    """Record both readings of cost and latency (ADR-010).

    The row's own `call_cost_usd`/`machine_s` are *execution-only* for C and D, which is exactly what makes C versus D
    a clean concurrency contrast: both replay the same plan, so the planning component is identical and cancels. The
    `system_*` fields add the block's planning back, because a deployment of either configuration would have paid for
    it — that is the number to quote against A/B, whose planning is already inside the run."""
    usage = plan_usage(plan)
    cost, machine = row.get("call_cost_usd"), row.get("machine_s")
    known = bool(row.get("cost_known")) and (plan is None or usage["cost_known"])
    row["plan_call_cost_usd"] = usage["cost_usd"]
    row["plan_machine_s"] = usage["seconds"]
    row["plan_tokens"] = usage["tokens"]
    row["system_cost_known"] = known
    row["system_call_cost_usd"] = None if cost is None or not known else round(float(cost) + (usage["cost_usd"] or 0.0), 6)
    row["system_machine_s"] = None if machine is None else round(float(machine) + usage["seconds"], 3)
    return row


def budget_exhausted_row(config: str, n: int, repetition: int, plan: dict, budgets: dict) -> dict:
    """The block's shared plan consumed the whole equal total budget, so there is nothing left to execute with.

    That is the experiment's budgets deciding the outcome — an experimental result (ABORTED/`BUDGET_EXHAUSTED`), not a
    machinery failure, and not something to paper over by handing the execution run a fresh budget."""
    return {
        "config": config,
        "n": n,
        "repetition": repetition,
        "status": "ABORTED",
        "verdict": "ABORTED:the block's shared plan consumed the run's total budget",
        "verdict_reason": "BUDGET_EXHAUSTED",
        "plan_failed": False,
        "budget_exhausted_by_plan": True,
        "execution_budgets": dict(budgets),
        "plan_sha256": plan.get("plan_sha256"),
        "plan_tasks": plan.get("tasks"),
        "cost_known": True,
        "call_cost_usd": 0.0,
        "model_calls": 0,
    }


def spend_ledger(rows: list[dict], plan_rows: list[dict]) -> dict:
    """Every priced dollar this experiment has already been billed, from the raw append-only logs (ADR-010).

    Superseded infrastructure rows and retried planning rounds are counted: the provider charged for those too, so the
    cap must see them. An operation that reported no known cost makes the total a floor rather than a total, and is
    counted separately — with a ceiling to enforce, unknown spend is a reason to stop, not to continue."""
    spent, unpriced, billing = 0.0, [], []
    for record in list(rows) + list(plan_rows):
        # bookkeeping rows for a cell that never executed (no shared plan, or the plan ate the budget) echo the
        # planning round's cost for classification; the planning round itself is already in the ledger
        if record.get("plan_failed") or record.get("budget_exhausted_by_plan"):
            continue
        billing.append(record)
        cost = record.get("call_cost_usd")
        if record.get("cost_known") is True and cost is not None:
            spent += float(cost)
        else:
            unpriced.append(record)
    return {"spent_usd": round(spent, 6), "unpriced_operations": len(unpriced), "operations": len(billing)}


def pending_operations(schedule: list[dict], effective: dict[tuple, dict], plans: dict[tuple[int, int], dict]) -> list[tuple]:
    """The billable operations this invocation still owes: `('plan', N, rep)` and `(config, N, rep)`, in schedule order.

    Resume rules, so the worst-case projection is honest: a cell with a valid effective row is done, an
    infrastructure-invalid one is owed again, and a block needs a planning round only when a replay config still owes a
    run and no usable plan is on record."""
    owed: list[tuple] = []
    for block in schedule:
        n, repetition = int(block["n"]), int(block["repetition"])
        cells = [
            c
            for c in block["configs"]
            if effective.get((c, n, repetition)) is None or classify_run(effective[(c, n, repetition)]) == "infrastructure"
        ]
        if any(c in REPLAY_CONFIGS for c in cells) and not (plans.get((n, repetition)) or {}).get("planned"):
            owed.append(("plan", n, repetition))
        owed += [(c, n, repetition) for c in cells]
    return owed


def spend_admission(
    ledger: dict,
    cap: float | None,
    next_max: float,
    owed: list[tuple],
    label: str,
    *,
    stop_on_unpriced: bool = True,
) -> tuple[str, str | None]:
    """(what to print before this operation, why the matrix must stop instead). ADR-010's admission rule.

    Stop *before* spending: an operation may only start if the ceiling still covers its own maximum, and unknown spend
    is never assumed to be zero — an operation that reported no priced cost makes the ceiling unenforceable, so the
    matrix stops unless the operator explicitly accepted unknown cost (`--allow-unpriced`)."""
    spent = float(ledger["spent_usd"])
    if cap is None:
        return f"[spend] billed ${spent:.4f} (no ceiling configured); {label} <= ${next_max:.4f}", None
    worst = spent + sum(next_max for _ in owed)
    line = (
        f"[spend] billed ${spent:.4f} of ${cap:.4f} ceiling; {label} <= ${next_max:.4f}; "
        f"{len(owed)} operation(s) left, worst case <= ${worst:.4f}"
    )
    if ledger["unpriced_operations"] and stop_on_unpriced:
        return line, (
            f"{ledger['unpriced_operations']} recorded operation(s) have unknown cost, so the ${cap:.4f} ceiling "
            "cannot be enforced (price every model in MAS_MODEL_PRICES, or accept it with --allow-unpriced)"
        )
    if spent + next_max > cap:
        return line, (
            f"the ${cap:.4f} ceiling does not cover another ${next_max:.4f} operation (billed ${spent:.4f}); "
            "raise --max-total-cost-usd deliberately to continue"
        )
    return line, None


def _pause(seconds: float, why: str) -> None:
    if seconds and seconds > 0:
        print(f"[pause] {seconds:g}s ({why})", flush=True)
        time.sleep(seconds)


def after_operation(infrastructure: bool, state: dict, args) -> str | None:
    """Pace between operations, cool down after a machinery failure, stop after too many in a row (ADR-010).

    Consecutive infrastructure failures mean the provider, Docker or the network is down — continuing would spend real
    money on runs that cannot answer anything, and would fill the log with cells to rerun. Returns the stop reason."""
    if not infrastructure:
        state["consecutive"] = 0
        _pause(args.pace_s, "pacing")
        return None
    state["consecutive"] += 1
    limit = int(args.max_consecutive_infrastructure or 0)
    if limit and state["consecutive"] >= limit:
        return (
            f"{state['consecutive']} consecutive infrastructure failures (limit {limit}): the machinery or the "
            "provider is unavailable. Rerun the same command to resume once it is healthy."
        )
    _pause(args.cooldown_s, "cooldown after an infrastructure failure")
    return None


def completion(rows: list[dict], *, configs: list[str], widths: list[int], repeats: int, require_priced: bool) -> dict:
    """Is the raw evidence complete? Computed from the rows alone (the gate recomputes it the same way).

    Every (config, N, repetition) must have exactly one effective row that is terminal, not infrastructure-invalid and
    (live) priced. Experimental failures count as complete evidence; infrastructure-invalid keys are listed for rerun."""
    expected_keys = {(c, n, r) for c in configs for n in widths for r in range(1, repeats + 1)}
    effective, audit = effective_rows(rows)
    actual_keys = set(effective)
    missing = sorted(expected_keys - actual_keys)
    foreign = sorted(actual_keys - expected_keys, key=str)
    valid_rows = [row for key, row in effective.items() if key in expected_keys]
    classes = {row_key(row): classify_run(row) for row in valid_rows}
    infra_keys = sorted((k for k, c in classes.items() if c == "infrastructure"), key=str)
    invalid = [r for r in valid_rows if r.get("status") not in TERMINAL]
    unpriced = [r for r in valid_rows if not r.get("cost_known", False)] if require_priced else []
    class_counts = {c: sum(1 for v in classes.values() if v == c) for c in FAILURE_CLASSES}
    evidence_complete = (
        not missing and not foreign and not audit["duplicates"] and not infra_keys and not invalid and not unpriced
    )
    return {
        "expected_runs": len(expected_keys),
        "recorded_runs": len(rows),
        "effective_runs": len(valid_rows),
        "evidence_complete": evidence_complete,
        "all_passed": evidence_complete and all(r.get("status") == "PASSED" for r in valid_rows),
        "missing": [{"config": c, "n": n, "repetition": r} for c, n, r in missing],
        "foreign_rows": len(foreign),
        "duplicate_rows": len(audit["duplicates"]),
        "superseded_rows": audit["superseded"],
        "invalid_rows": len(invalid),
        "unpriced_rows": len(unpriced),
        "infrastructure_invalid_rows": len(infra_keys),
        "keys_needing_rerun": [{"config": c, "n": n, "repetition": r} for c, n, r in infra_keys],
        "failure_classes": class_counts,
    }


def command(args, config: str, n: int, dag_file: Path, plan_file: Path | None = None, budgets: dict | None = None) -> list[str]:
    """The `mas run` argv for one cell. `budgets` is what this run may still spend of its equal total budget (ADR-010);
    without it the full per-run budget is used (A/B, and the dry-run listing)."""
    budgets = budgets or full_budgets(args)
    common = [
        sys.executable,
        "-m",
        "mas",
        "run",
        "--config",
        config,
        "--benchmark",
        f"adapters_{n}",
        "--max-tokens",
        str(budgets["max_tokens"]),
        "--max-attempt-tokens",
        str(args.max_attempt_tokens),
        "--max-cost-usd",
        str(budgets["max_cost_usd"]),
        "--max-wallclock-s",
        str(budgets["max_wallclock_s"]),
        "--max-replans",
        str(args.max_replans),
    ]
    if args.offline:
        return common + [
            "--dag",
            str(plan_file if config in REPLAY_CONFIGS and plan_file is not None else dag_file),
            "--agent",
            "stub",
            "--workers",
            str(n if config == "D" else 1),
            "--max-concurrency",
            str(n if config == "D" else 1),
        ]
    if config in {"A", "B"}:
        model = args.cheap_model if config == "A" else args.strong_model
        return common + [
            "--dag",
            str(dag_file),
            "--agent",
            "llm",
            "--model",
            model,
            "--exec-backend",
            "sandbox",
            "--workers",
            "1",
            "--max-concurrency",
            "1",
        ]
    # C and D replay the block's ONE validated plan (ADR-009); `--planner llm` stays for bounded repair amendments
    if plan_file is None:
        raise ValueError(f"config {config} needs the block's shared plan")
    concurrency = 1 if config == "C" else n
    return common + [
        "--dag",
        str(plan_file),
        "--planner",
        "llm",
        "--planner-model",
        args.planner_model,
        "--agent",
        "llm",
        "--model",
        args.worker_model,
        "--exec-backend",
        "sandbox",
        "--workers",
        str(concurrency),
        "--max-concurrency",
        str(concurrency),
    ]


def run_one(argv: list[str], *, config: str, n: int, repetition: int) -> dict:
    print(f"\n[{config} N={n} repeat={repetition}] " + " ".join(argv[3:]), flush=True)
    proc = subprocess.run(argv, cwd=ROOT, text=True, capture_output=True, check=False)
    print(proc.stdout, end="")
    if proc.stderr:
        print(proc.stderr, file=sys.stderr, end="")
    match = RUN_ID.search(proc.stdout)
    row = {"config": config, "n": n, "repetition": repetition, "exit_code": proc.returncode}
    if not match:
        row.update({"status": "CLIENT_ERROR", "error": "run id not found in CLI output"})
        row["failure_class"] = classify_run(row)
        return row
    run_id = UUID(match.group(1))
    with connect() as conn:
        m = compute(conn, run_id)
    row.update(m.as_dict())
    row["cost_known"] = m.unpriced_calls == 0
    if m.unpriced_calls:
        row["cost_usd"] = None
        row["call_cost_usd"] = None
    row["failure_class"] = classify_run(row)
    return row


def _derived(row: dict) -> dict:
    """Per-run values that evaluation.md §4 asks for but that are ratios of recorded counters."""
    attempts = row.get("attempts") or 0
    calls = row.get("model_calls") or 0
    return {
        "tokens_in_per_attempt": (float(row["call_input_tokens"]) / attempts)
        if attempts and row.get("call_input_tokens") is not None
        else None,
        "call_latency_s": (float(row["call_seconds"]) / calls) if calls and row.get("call_seconds") is not None else None,
    }


def _values(rows: list[dict], field: str) -> list[float]:
    out: list[float] = []
    for r in rows:
        v = r.get(field)
        if v is None:
            v = _derived(r).get(field)
        if v is not None:
            out.append(float(v))
    return out


def distribution(values: list[float]) -> dict | None:
    """min / p25 / median / p75 / max / mean over the given values (None when there are none)."""
    if not values:
        return None
    ordered = sorted(values)
    last = len(ordered) - 1
    out = {"n": len(ordered)}
    for q, name in _PERCENTILES:
        pos = q * last
        lo, hi = int(pos // 1), min(last, int(pos // 1) + 1)
        out[name] = round(ordered[lo] + (ordered[hi] - ordered[lo]) * (pos - lo), 6)
    out["mean"] = round(statistics.fmean(ordered), 6)
    return out


def _histogram(rows: list[dict], getter) -> dict[str, int]:
    out: dict[str, int] = {}
    for r in rows:
        key = getter(r)
        key = "none" if key in (None, "") else str(key)
        out[key] = out.get(key, 0) + 1
    return dict(sorted(out.items()))


def aggregate(rows: list[dict]) -> list[dict]:
    """One record per (config, N) over the *effective* rows: headline medians over passing runs (the crossover
    question), distributions over every effective run, and failure/task-shape histograms."""
    effective, _ = effective_rows(rows)
    valid = list(effective.values())
    out = []
    for config in CONFIGS:
        for n in WIDTHS:
            cell = [r for r in valid if r["config"] == config and r["n"] == n]
            if not cell:
                continue
            passed = [r for r in cell if r.get("status") == "PASSED"]
            infra = [r for r in cell if classify_run(r) == "infrastructure"]
            evidence = [r for r in cell if classify_run(r) != "infrastructure"]
            times = [float(r["machine_s"]) for r in passed if r.get("machine_s") is not None]
            costs = [float(r["call_cost_usd"]) for r in passed if r.get("call_cost_usd") is not None]
            record: dict[str, Any] = {
                "config": config,
                "n": n,
                "runs": len(cell),
                "evidence_runs": len(evidence),
                "infrastructure_invalid": len(infra),
                "passes": len(passed),
                "success_rate": round(len(passed) / len(evidence), 4) if evidence else None,
                "median_machine_s": round(statistics.median(times), 3) if times else None,
                "median_cost_usd": round(statistics.median(costs), 6) if len(costs) == len(passed) and costs else None,
            }
            for field, _label in DISTRIBUTION_FIELDS:
                dist = distribution(_values(evidence, field))
                record[f"median_{field}"] = None if dist is None else dist["median"]
            record["distributions"] = {field: distribution(_values(evidence, field)) for field, _ in DISTRIBUTION_FIELDS}
            record["failure_classes"] = _histogram(evidence, classify_run)
            failed = [r for r in evidence if r.get("status") != "PASSED"]
            record["verdict_reasons"] = _histogram(failed, lambda r: r.get("verdict_reason"))
            record["attempt_failure_classes"] = _sum_dicts(r.get("attempt_failure_classes") for r in evidence)
            record["suggested_modes"] = _histogram(evidence, lambda r: _shape(r).get("suggested_mode"))
            record["estimated_widths"] = _histogram(evidence, lambda r: _shape(r).get("estimated_width"))
            out.append(record)
    return out


def _shape(row: dict) -> dict:
    shape = row.get("task_shape")
    return shape if isinstance(shape, dict) else {}


def _sum_dicts(dicts) -> dict[str, int]:
    out: dict[str, int] = {}
    for d in dicts:
        if isinstance(d, dict):
            for k, v in d.items():
                out[str(k)] = out.get(str(k), 0) + int(v)
    return dict(sorted(out.items()))


def scalar_summary(summary: list[dict]) -> list[dict]:
    """The flat (CSV-friendly) part of each aggregate record."""
    return [{k: v for k, v in record.items() if not isinstance(v, dict | list)} for record in summary]


def write_svg(summary: list[dict], path: Path) -> None:
    colors = {"A": "#4e79a7", "B": "#f28e2b", "C": "#59a14f", "D": "#e15759"}
    panels = [("median_machine_s", "Machine seconds"), ("median_cost_usd", "Cost USD"), ("success_rate", "Success rate")]
    width, height = 1080, 360
    chunks = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
    ]
    for panel, (field, title) in enumerate(panels):
        x0, y0, pw, ph = 30 + panel * 360, 45, 310, 250
        values = [float(r[field]) for r in summary if r.get(field) is not None]
        ymax = max(values or [1.0]) or 1.0
        chunks += [
            f'<text x="{x0}" y="25" font-family="sans-serif" font-size="16">{title}</text>',
            f'<path d="M{x0},{y0} V{y0 + ph} H{x0 + pw}" fill="none" stroke="#777"/>',
        ]
        for config in CONFIGS:
            pts = []
            for r in summary:
                if r["config"] != config or r.get(field) is None:
                    continue
                x = x0 + WIDTHS.index(r["n"]) * pw / (len(WIDTHS) - 1)
                y = y0 + ph - float(r[field]) / ymax * ph
                pts.append((x, y))
                chunks.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3" fill="{colors[config]}"/>')
            if pts:
                points = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
                chunks.append(f'<polyline points="{points}" fill="none" stroke="{colors[config]}" stroke-width="2"/>')
        for i, n in enumerate(WIDTHS):
            x = x0 + i * pw / (len(WIDTHS) - 1)
            chunks.append(
                f'<text x="{x:.1f}" y="{y0 + ph + 18}" text-anchor="middle" font-family="sans-serif" font-size="11">{n}</text>'
            )
    for i, config in enumerate(CONFIGS):
        chunks.append(
            f'<text x="{30 + i * 70}" y="345" fill="{colors[config]}" font-family="sans-serif" font-size="13">{config}</text>'
        )
    chunks.append("</svg>")
    path.write_text("\n".join(chunks) + "\n", encoding="utf-8")


def crossover(summary: list[dict], baseline: str, field: str) -> int | None:
    """First width where D is at least as successful and strictly better on the selected lower-is-better metric."""
    by = {(r["config"], r["n"]): r for r in summary}
    for n in WIDTHS:
        base, parallel = by.get((baseline, n)), by.get(("D", n))
        if not base or not parallel or base.get(field) is None or parallel.get(field) is None:
            continue
        if parallel["success_rate"] >= base["success_rate"] and float(parallel[field]) < float(base[field]):
            return n
    return None


def _fmt(value, digits: int = 3) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.{digits}f}".rstrip("0").rstrip(".") if value != int(value) else str(int(value))
    return str(value)


def _dist_cell(dist: dict | None) -> str:
    """`median [min, p25 .. p75, max] (n)` — the distribution, not a point."""
    if not dist:
        return "-"
    spread = f"[{_fmt(dist['min'])}, {_fmt(dist['p25'])} .. {_fmt(dist['p75'])}, {_fmt(dist['max'])}]"
    return f"{_fmt(dist['median'])} {spread} (n={dist['n']})"


def render_analysis(summary: list[dict], manifest: dict, done: dict, plans: dict[tuple[int, int], dict] | None = None) -> str:
    """The M3 report (evaluation.md §4/§7) as Markdown. Deterministic in (summary, manifest, done, plans): the MVP gate
    regenerates it from the raw rows and requires the file on disk to match byte for byte."""
    by = {(r["config"], r["n"]): r for r in summary}
    spec = manifest["spec"]
    widths, configs = list(spec["widths"]), list(spec["configs"])
    env = spec.get("environment") or {}
    lines = [
        "# Frozen M3 result",
        "",
        f"Experiment: `{manifest['experiment_id']}`  ",
        f"Git commit: `{manifest['git_commit']}`  ",
        f"Mode: `{spec.get('mode')}`; models: `{json.dumps(spec.get('models'), sort_keys=True)}`  ",
        f"Budgets per run: `{json.dumps(spec.get('budgets'), sort_keys=True)}`  ",
        f"Schedule: randomized blocks, seed `{spec.get('seed')}` (order frozen in `experiment.json`)  ",
        f"Aggregate spend ceiling: `{manifest.get('spend_cap_usd')}` USD "
        f"({len(manifest.get('spend_cap_history') or [])} recorded setting(s), ADR-010)  ",
        f"Environment: python `{env.get('python')}` on `{env.get('platform')}`; "
        f"exec image `{str((env.get('exec') or {}).get('image_id'))[:19]}`, "
        f"verifier image `{str((env.get('verifier') or {}).get('image_id'))[:19]}`  ",
        f"Evidence complete: **{done['evidence_complete']}** "
        f"({done.get('effective_runs', done['recorded_runs'])}/{done['expected_runs']} effective runs; "
        f"{done['recorded_runs']} rows recorded, {done.get('superseded_rows', 0)} infrastructure-invalid rows superseded by "
        f"reruns, {done.get('infrastructure_invalid_rows', 0)} still needing a rerun)  ",
        f"All runs passed: **{done['all_passed']}**",
        "",
        "Failure classification: `experimental` runs (the model, the plan or the run's budgets decided the outcome) are "
        "evidence and are kept; `infrastructure` runs (verifier crash/timeout, unusable suite, provider outage, "
        "sandbox/workspace failure, worker death, lost client) cannot answer the value question and are rerun. Success "
        "rate is over "
        "evidence runs; headline medians use passing runs; distributions use every evidence run in the cell.",
        "",
        "## Headline: A/B/C/D at each N",
        "",
        "| N | Config | Evidence runs | Infra-invalid | Success | Median machine s | Median wall-clock s | "
        "Median critical path s | Median cost USD |",
        "|---:|:------:|-----:|-----:|--------:|-----------------:|-----------------:|-----------------:|----------------:|",
    ]
    for n in widths:
        for config in configs:
            row = by.get((config, n), {})
            success = "-" if row.get("success_rate") is None else f"{100 * row['success_rate']:.1f}%"
            lines.append(
                f"| {n} | {config} | {row.get('evidence_runs', 0)} | {row.get('infrastructure_invalid', 0)} | {success} | "
                f"{_fmt(row.get('median_machine_s'))} | {_fmt(row.get('median_wall_clock_s'))} | "
                f"{_fmt(row.get('median_critical_path_s'))} | {_fmt(row.get('median_cost_usd'), 6)} |"
            )
    lines += [
        "",
        "## Shared plans (paired C/D evidence, ADR-009)",
        "",
        "One validated plan per (N, repetition) block, produced under the parallel budget and executed by **both** C "
        "and D — so C versus D differs in concurrency alone. Planning cost is charged to the plan-only run below, "
        "once per block, and is therefore *not* inside any C or D run. Two readings follow from that (ADR-010): the "
        "per-run `call_cost_usd` / `machine_s` are **execution-only** — the right C-versus-D contrast, because the "
        "shared planning component is identical and cancels — while `system_call_cost_usd` / `system_machine_s` add "
        "the block's planning back and are the numbers to quote against A/B, whose planning is inside their run. C "
        "and D also execute under what the shared plan *left* of the equal total budget, so no configuration receives "
        "more than another.",
        "",
        "| N | Rep | Plan | Tasks | Outcome | Planner cost USD | Planner s | Calls | Rejections |",
        "|---:|---:|:---|---:|:---|---:|---:|---:|---:|",
    ]
    for n in widths:
        for repetition in range(1, int(spec.get("repeats") or 0) + 1):
            plan = (plans or {}).get((n, repetition))
            if plan is None:
                lines.append(f"| {n} | {repetition} | - | - | not planned | - | - | - | - |")
                continue
            digest = str(plan.get("plan_sha256") or "-")
            lines.append(
                f"| {n} | {repetition} | `{digest[:12]}` | {_fmt(plan.get('tasks'))} | {plan.get('outcome') or '-'} | "
                f"{_fmt(plan.get('call_cost_usd'), 6)} | {_fmt(plan.get('plan_s'))} | {_fmt(plan.get('model_calls'))} | "
                f"{_fmt(plan.get('plan_rejections'))} |"
            )
    lines += ["", "## Parallel MAS crossover", ""]
    for baseline in ("A", "B", "C"):
        time_n = crossover(summary, baseline, "median_machine_s")
        cost_n = crossover(summary, baseline, "median_cost_usd")
        lines.append(
            f"- D versus {baseline}: first success-preserving time crossover = "
            f"{f'N={time_n}' if time_n else 'none observed'}; cost crossover = "
            f"{f'N={cost_n}' if cost_n else 'none observed'}."
        )
    lines += [
        "",
        "## Distributions per cell",
        "",
        "Each cell shows `median [min, p25 .. p75, max] (n)` over the cell's evidence runs.",
    ]
    for field, label in DISTRIBUTION_FIELDS:
        header = "| N | " + " | ".join(configs) + " |"
        rule = "|---:|" + "|".join(":---" for _ in configs) + "|"
        lines += ["", f"### {label} (`{field}`)", "", header, rule]
        for n in widths:
            cells = []
            for config in configs:
                dists = by.get((config, n), {}).get("distributions") or {}
                cells.append(_dist_cell(dists.get(field)))
            lines.append(f"| {n} | " + " | ".join(cells) + " |")
    lines += [
        "",
        "## Failures",
        "",
        "| N | Config | Failure classes | Verdict reasons (non-passing) | Attempt failure classes |",
        "|---:|:------:|:---|:---|:---|",
    ]
    for n in widths:
        for config in configs:
            row = by.get((config, n), {})
            lines.append(
                f"| {n} | {config} | {json.dumps(row.get('failure_classes') or {}, sort_keys=True)} | "
                f"{json.dumps(row.get('verdict_reasons') or {}, sort_keys=True)} | "
                f"{json.dumps(row.get('attempt_failure_classes') or {}, sort_keys=True)} |"
            )
    lines += [
        "",
        "## Task shape (advisory planner metadata vs configured mode)",
        "",
        "The configuration column is what ran (frozen A/B/C/D); the planner's suggested mode and estimated width are "
        "recorded but never selected the mode (ADR-008).",
        "",
        "| N | Config | Planner-suggested modes | Planner-estimated widths |",
        "|---:|:------:|:---|:---|",
    ]
    for n in widths:
        for config in configs:
            row = by.get((config, n), {})
            lines.append(
                f"| {n} | {config} | {json.dumps(row.get('suggested_modes') or {}, sort_keys=True)} | "
                f"{json.dumps(row.get('estimated_widths') or {}, sort_keys=True)} |"
            )
    lines += [
        "",
        "This report is descriptive. A negative result is a completed MVP result; do not tune or omit cells after "
        "seeing outcomes.",
    ]
    return "\n".join(lines) + "\n"


def write_analysis(summary: list[dict], manifest: dict, done: dict, plans: dict[tuple[int, int], dict], path: Path) -> None:
    path.write_text(render_analysis(summary, manifest, done, plans), encoding="utf-8")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cheap-model")
    ap.add_argument("--strong-model")
    ap.add_argument("--planner-model")
    ap.add_argument("--worker-model")
    ap.add_argument("--offline", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--allow-dirty", action="store_true", help="allow a live experiment from an uncommitted tree")
    ap.add_argument("--allow-unpriced", action="store_true", help="record live results even when cost is unknown")
    ap.add_argument("--repeats", type=int, default=5)
    ap.add_argument("--widths", type=int, nargs="+", default=list(WIDTHS), choices=WIDTHS)
    ap.add_argument("--configs", nargs="+", default=list(CONFIGS), choices=CONFIGS)
    ap.add_argument("--output", type=Path, default=ROOT / "benchmark-results")
    ap.add_argument("--max-tokens", type=int, default=5_000_000)
    ap.add_argument("--max-attempt-tokens", type=int, default=250_000)
    ap.add_argument("--max-cost-usd", type=float, default=20.0)
    ap.add_argument("--max-wallclock-s", type=int, default=1800)
    ap.add_argument("--max-replans", type=int, default=1)
    ap.add_argument("--seed", type=int, default=20260817, help="draws the block/config execution order (frozen in the manifest)")
    ap.add_argument(
        "--max-total-cost-usd",
        type=float,
        default=None,
        help="aggregate ceiling for the WHOLE matrix (ADR-010); required live. Recomputed from the raw logs before "
        "every operation, which may only start if the ceiling still covers its own maximum",
    )
    ap.add_argument("--pace-s", type=float, default=0.0, help="wait this long between operations (provider pacing)")
    ap.add_argument("--cooldown-s", type=float, default=60.0, help="wait this long after an infrastructure failure")
    ap.add_argument(
        "--max-consecutive-infrastructure",
        type=int,
        default=3,
        help="stop the matrix after this many machinery/provider failures in a row (0 disables the circuit breaker)",
    )
    args = ap.parse_args(argv)
    if args.repeats < 1:
        ap.error("--repeats must be positive")
    if args.max_total_cost_usd is not None:
        if args.max_total_cost_usd <= 0:
            ap.error("--max-total-cost-usd must be positive")
        if args.max_total_cost_usd < args.max_cost_usd:
            # otherwise the admission rule refuses the very first operation: no ceiling can cover one run's maximum
            ap.error(
                f"--max-total-cost-usd {args.max_total_cost_usd} is below the per-run --max-cost-usd "
                f"{args.max_cost_usd}: no operation could ever start"
            )
    if not args.offline and not args.dry_run:
        missing = [name for name in ("cheap_model", "strong_model", "planner_model", "worker_model") if not getattr(args, name)]
        if missing:
            ap.error("real matrix needs " + ", ".join("--" + x.replace("_", "-") for x in missing))
        if args.repeats < 5:
            ap.error("the frozen M3 evaluation requires --repeats >= 5 (use --offline for a rehearsal)")
        _, dirty = _git_state()
        if dirty and not args.allow_dirty:
            ap.error("real matrix requires a clean Git tree so results identify exact code (or explicitly use --allow-dirty)")
        if not settings().model_prices.strip() and not args.allow_unpriced:
            ap.error("real matrix requires MAS_MODEL_PRICES so cost is measurable (or explicitly use --allow-unpriced)")
        if args.max_total_cost_usd is None:
            ap.error(
                "real matrix requires --max-total-cost-usd: without an aggregate ceiling the worst case is "
                f"{len(args.configs) * len(args.widths) * args.repeats} runs + planning rounds at "
                f"${args.max_cost_usd} each (ADR-010)"
            )
        # the request shape is frozen in the manifest, so it must be a deliberate choice before the first block runs
        if any(str(getattr(args, f"{r}_model") or "").startswith("anthropic:") for r in ("cheap", "strong", "planner", "worker")):
            if not settings().anthropic_effort.strip():
                ap.error(
                    "set MAS_ANTHROPIC_EFFORT explicitly (e.g. 'medium') before a live matrix: reasoning effort drives "
                    "cost and behavior, it is frozen in experiment.json, and it cannot change mid-experiment (ADR-010)"
                )
    if args.dry_run:
        manifest = {
            "experiment_id": "dry-run",
            "schedule": build_schedule(list(args.configs), list(args.widths), args.repeats, args.seed),
        }
        rows: list[dict] = []
        plan_rows: list[dict] = []
        plans: dict[tuple[int, int], dict] = {}
    else:
        args.output.mkdir(parents=True, exist_ok=True)
        try:
            manifest = open_experiment(args.output, experiment_spec(args), spend_cap_usd=args.max_total_cost_usd)
            rows = load_rows(args.output / "runs.jsonl", manifest["experiment_id"])
            plan_rows = load_rows(args.output / "plans.jsonl", manifest["experiment_id"])
            plans = load_plans(args.output / "plans.jsonl", manifest["experiment_id"])
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            ap.error(str(exc))
    jsonl = args.output / "runs.jsonl"
    plans_jsonl = args.output / "plans.jsonl"
    plans_dir = args.output / "plans"
    effective, _audit = effective_rows(rows)
    if not args.dry_run:
        plans_dir.mkdir(parents=True, exist_ok=True)
    cap = manifest.get("spend_cap_usd") if not args.dry_run else args.max_total_cost_usd
    breaker = {"consecutive": 0}
    stop_reason: str | None = None
    # the frozen randomized schedule (ADR-009): one (N, repetition) block at a time, its configs in the drawn order
    for block in manifest["schedule"]:
        if stop_reason:
            break
        n, repetition, order = int(block["n"]), int(block["repetition"]), list(block["configs"])
        needs_plan = any(c in REPLAY_CONFIGS for c in order)
        plan_file = plans_dir / f"n{n:02d}-r{repetition}.json"
        fixture = plans_dir / f"adapters_{n}.json"  # A/B: the hand-written width DAG, collapsed to one solve task
        if args.dry_run:
            if needs_plan:
                made = plan_command(args, n) + ["--output", str(plan_file)]
                print(f"[fixture] {plan_file}" if args.offline else " ".join(made))
            for config in order:
                print(" ".join(command(args, config, n, fixture, plan_file)))
            continue
        fixture.write_text(json.dumps(width_dag(n).to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
        pending = [
            c
            for c in order
            if effective.get((c, n, repetition)) is None or classify_run(effective[(c, n, repetition)]) == "infrastructure"
        ]
        if needs_plan and any(c in REPLAY_CONFIGS for c in pending):
            ledger = spend_ledger(rows, plan_rows)
            owed = pending_operations(manifest["schedule"], effective, plans)
            line, refusal = spend_admission(
                ledger,
                cap,
                float(args.max_cost_usd),
                owed,
                f"plan N={n} repeat={repetition}",
                stop_on_unpriced=not args.allow_unpriced,
            )
            print(line, flush=True)
            if refusal:
                stop_reason = refusal
                break
            produced = block_plan(args, plans, n, repetition, plans_dir)
            if produced is not plans.get((n, repetition)):  # newly produced (or retried): append it to the evidence
                record = dict(produced, experiment_id=manifest["experiment_id"], git_commit=manifest["git_commit"])
                plans[(n, repetition)] = record
                plan_rows.append(record)
                with plans_jsonl.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(record, default=str) + "\n")
                stop_reason = after_operation(record.get("outcome") == "client_error", breaker, args)
                if stop_reason:
                    break
        plan = plans.get((n, repetition))
        for config in order:
            key = (config, n, repetition)
            previous = effective.get(key)
            if previous is not None:
                if classify_run(previous) != "infrastructure":
                    print(f"[resume] {config} N={n} repeat={repetition} already recorded", flush=True)
                    continue
                why = previous.get("verdict") or previous.get("error") or previous.get("status")
                print(
                    f"[rerun] {config} N={n} repeat={repetition}: previous run {previous.get('run_id') or '?'} "
                    f"was infrastructure-invalid ({why})",
                    flush=True,
                )
            shared = plan if config in REPLAY_CONFIGS else None
            budgets = execution_budgets(args, shared)
            executed = False
            if config in REPLAY_CONFIGS and not (plan or {}).get("planned"):
                row = plan_failure_row(config, n, repetition, plan or {})
                print(f"[skip] {config} N={n} repeat={repetition}: no shared plan ({row['verdict']})", flush=True)
            elif min(budgets["max_tokens"], budgets["max_cost_usd"], budgets["max_wallclock_s"]) <= 0:
                row = budget_exhausted_row(config, n, repetition, plan or {}, budgets)
                print(f"[skip] {config} N={n} repeat={repetition}: {row['verdict']} ({budgets})", flush=True)
            else:
                ledger = spend_ledger(rows, plan_rows)
                owed = pending_operations(manifest["schedule"], effective, plans)
                line, refusal = spend_admission(
                    ledger,
                    cap,
                    float(budgets["max_cost_usd"]),
                    owed,
                    f"{config} N={n} repeat={repetition}",
                    stop_on_unpriced=not args.allow_unpriced,
                )
                print(line, flush=True)
                if refusal:
                    stop_reason = refusal
                    break
                row = run_one(command(args, config, n, fixture, plan_file, budgets), config=config, n=n, repetition=repetition)
                executed = True
                row["execution_budgets"] = budgets
                if config in REPLAY_CONFIGS:
                    row["plan_sha256"] = (plan or {}).get("plan_sha256")
                    row["plan_tasks"] = (plan or {}).get("tasks")
            attach_system_totals(row, shared)
            row["experiment_id"] = manifest["experiment_id"]
            row["git_commit"] = manifest["git_commit"]
            row["suite_sha256"] = manifest["spec"]["suite_sha256"][str(n)]
            row["schedule_index"] = int(block["index"])
            if previous is not None:
                row["rerun_of"] = previous.get("run_id")
                row["rerun_index"] = int(previous.get("rerun_index") or 0) + 1
            rows.append(row)
            effective[key] = row
            with jsonl.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, default=str) + "\n")
            if executed:  # only a real operation paces or trips the breaker; bookkeeping rows called no provider
                stop_reason = after_operation(classify_run(row) == "infrastructure", breaker, args)
                if stop_reason:
                    break
    if args.dry_run:
        return 0
    summary = aggregate(rows)
    flat = scalar_summary(summary)
    with (args.output / "summary.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(flat[0]) if flat else ["config", "n"])
        writer.writeheader()
        writer.writerows(flat)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    write_svg(summary, args.output / "report.svg")
    done = completion(
        rows,
        configs=list(args.configs),
        widths=list(args.widths),
        repeats=args.repeats,
        require_priced=not args.offline and not args.allow_unpriced,
    )
    done.update(
        {
            "schema": 1,
            "experiment_id": manifest["experiment_id"],
            "updated_at": datetime.now(UTC).isoformat(),
        }
    )
    (args.output / "completion.json").write_text(json.dumps(done, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_analysis(summary, manifest, done, plans, args.output / "analysis.md")
    ledger = spend_ledger(rows, plan_rows)
    print(
        f"\nresults: {args.output} ({done['effective_runs']}/{done['expected_runs']} effective runs, {len(rows)} rows; "
        f"evidence_complete={done['evidence_complete']}; all_passed={done['all_passed']}; "
        f"failure_classes={done['failure_classes']}; needing_rerun={len(done['keys_needing_rerun'])})"
    )
    print(
        f"spend: ${ledger['spent_usd']:.4f} billed over {ledger['operations']} recorded operation(s)"
        + (f" of a ${float(cap):.4f} ceiling" if cap is not None else " (no ceiling configured)")
        + (f"; {ledger['unpriced_operations']} with unknown cost" if ledger["unpriced_operations"] else "")
    )
    if done["keys_needing_rerun"]:
        print("infrastructure-invalid cells will be rerun on the next invocation with the same arguments")
    if stop_reason:
        print(f"\n[stop] {stop_reason}")
        print("the experiment is resumable: nothing was discarded, rerun the same command to continue")
        return 1
    # A model/task failure is valid experimental data. Missing/duplicate/infrastructure/unpriced evidence is not.
    return 0 if done["evidence_complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
