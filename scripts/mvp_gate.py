"""Audit whether the frozen MVP has complete, reproducible live evidence.

This does not call a provider or spend money. The three evidence producers are:

  python scripts/live_smoke.py --worker ... --planner ... --step all --no-auto-approve \
      --output mvp-evidence/live-smoke.json
  python scripts/distributed_smoke.py --build --output mvp-evidence/distributed-smoke.json
  python scripts/benchmark.py --cheap-model ... --strong-model ... --planner-model ... --worker-model ... \
      --repeats 5 --output benchmark-results

Then:

  python scripts/mvp_gate.py

Exit 0 means the direct live-model gate, distributed operator gate, and frozen M3 100-run matrix are complete on the
same clean commit — which must be the commit currently checked out, with a clean tree — with priced calls and a
manually approved acceptance contract. It does not require MAS to win.

The auditor trusts raw evidence, not summaries: it reloads `runs.jsonl` and `plans.jsonl`, checks every row's
experiment id, commit and suite identity, recomputes completion (missing / duplicate / infrastructure-invalid /
unpriced cells) and regenerates `analysis.md` from the rows, and requires `completion.json` and the report on disk to
agree with what it recomputed. It also audits the experiment's *design* (ADR-009): a frozen randomized block schedule
covering every cell, a frozen environment fingerprint, and one recorded plan per block that configs C and D both
executed — without that, a C-versus-D difference does not isolate concurrency.

Protocol audits (ADR-010): the schedule must be exactly what its seed draws *and* the rows must show it was followed
(every row carries its block's `schedule_index`, first-pass rows in schedule order); an aggregate spend ceiling must be
recorded, with the spend recomputed from the raw logs inside it and no operation of unknown cost; and where an
Anthropic model was used, the reasoning effort must have been an explicit frozen choice.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.benchmark import (  # noqa: E402
    REPLAY_CONFIGS,
    aggregate,
    build_schedule,
    completion,
    load_plans,
    load_rows,
    render_analysis,
    schedule_cells,
    spend_ledger,
)

CONFIGS = {"A", "B", "C", "D"}
WIDTHS = {1, 2, 4, 8, 16}
# completion.json fields the gate recomputes from the raw rows and requires to match exactly
_RECOMPUTED_COMPLETION_FIELDS = (
    "evidence_complete",
    "all_passed",
    "expected_runs",
    "recorded_runs",
    "effective_runs",
    "duplicate_rows",
    "superseded_rows",
    "invalid_rows",
    "unpriced_rows",
    "infrastructure_invalid_rows",
)


@dataclass(frozen=True)
class Check:
    name: str
    ok: bool
    detail: str


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"missing evidence: {path}") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read evidence {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"evidence must be a JSON object: {path}")
    return value


def current_git_state() -> tuple[str, bool]:
    """(HEAD commit, tree dirty?) of this checkout — the revision the evidence must be bound to."""
    rev = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=False)
    dirty = subprocess.run(["git", "status", "--porcelain"], cwd=ROOT, capture_output=True, text=True, check=False)
    return (rev.stdout.strip() if rev.returncode == 0 else "unknown", bool(dirty.stdout.strip()) or dirty.returncode != 0)


def _schedule_followed(schedule: list[dict[str, Any]], rows: list[dict[str, Any]]) -> str | None:
    """Did the evidence actually come out in the frozen order? (ADR-009/ADR-010)

    Two claims, both auditable from the rows alone: every row carries the `schedule_index` of *its* block, and the
    first-pass rows (those that are not reruns of an infrastructure-invalid cell) appear in schedule order. A recorded
    schedule nobody followed is decoration; reruns are appended out of order by construction and are exempt."""
    if not schedule:
        return "no schedule to audit"
    index_of = {(int(b["n"]), int(b["repetition"])): int(b["index"]) for b in schedule}
    position = {cell: i for i, cell in enumerate(schedule_cells(schedule))}
    mislabelled, order = [], []
    for number, row in enumerate(rows, 1):
        key = (row.get("config"), row.get("n"), row.get("repetition"))
        block = index_of.get((row.get("n"), row.get("repetition")))
        if block is None or row.get("schedule_index") != block:
            mislabelled.append(number)
        elif not row.get("rerun_of") and key in position:
            order.append(position[key])
    if mislabelled:
        return f"{len(mislabelled)} row(s) carry no/!= schedule_index of their block (first at line {mislabelled[0]})"
    if order != sorted(order):
        return "rows were not produced in the frozen schedule order"
    return None


def _design_checks(
    spec: dict[str, Any],
    manifest: dict[str, Any],
    rows: list[dict[str, Any]],
    plans: dict[tuple[int, int], dict[str, Any]],
    plans_error: str | None,
    *,
    configs: list[str],
    widths: list[int],
    repeats: int,
    plan_rows: list[dict[str, Any]],
) -> list[Check]:
    """ADR-009/ADR-010: the experiment's *design* is part of the evidence — a frozen randomized schedule that was
    actually followed, a frozen environment, one shared plan per block that C and D both executed, an explicit
    reasoning effort, and an aggregate spend ceiling the recomputed spend stayed inside."""
    schedule = manifest.get("schedule") if isinstance(manifest.get("schedule"), list) else []
    expected_cells = sorted((c, n, r) for c in configs for n in widths for r in range(1, repeats + 1))
    scheduled = sorted(schedule_cells(schedule)) if schedule else []
    seed = spec.get("seed")
    redrawn = build_schedule(configs, widths, repeats, int(seed)) if seed is not None and configs and widths else []
    followed = _schedule_followed(schedule, rows)
    env = spec.get("environment") if isinstance(spec.get("environment"), dict) else {}
    env_missing = [k for k in ("python", "platform", "provider", "attempt", "exec", "verifier") if not env.get(k)]
    blocks = [(n, r) for n in widths for r in range(1, repeats + 1)]
    # a block must have a *recorded* planning outcome. Not that planning succeeded: a planner that could not produce a
    # valid plan for one block is an experimental result, and demanding success here would push the operator to re-roll
    # the planner until it did — the exact bias this gate exists to prevent.
    unrecorded = [b for b in blocks if not plans.get(b)]
    failed_blocks = [b for b in blocks if plans.get(b) and not plans[b].get("planned")]
    by_key = {(row.get("config"), row.get("n"), row.get("repetition")): row for row in rows}
    unpaired: list[str] = []
    for block in blocks:
        record = plans.get(block) or {}
        rows_by_config = {c: (by_key.get((c, *block)) or {}) for c in REPLAY_CONFIGS}
        if record.get("planned"):
            digest = record.get("plan_sha256")
            seen = {c: r.get("plan_sha256") for c, r in rows_by_config.items()}
            if not digest or any(v != digest for v in seen.values()):
                unpaired.append(f"N={block[0]} r={block[1]} {seen} != {str(digest)[:12]}")
        elif not all(r.get("plan_failed") for r in rows_by_config.values()):
            # no plan for this block: C and D must both record exactly that, and neither may have run anyway
            unpaired.append(f"N={block[0]} r={block[1]} ran without the block's plan")
    models = spec.get("models") if isinstance(spec.get("models"), dict) else {}
    anthropic = [k for k, v in models.items() if str(v).startswith("anthropic:")]
    effort = str(((env.get("provider") or {}) if isinstance(env.get("provider"), dict) else {}).get("anthropic_effort") or "")
    cap = manifest.get("spend_cap_usd")
    ledger = spend_ledger(rows, plan_rows)
    within_cap = isinstance(cap, int | float) and cap > 0 and ledger["spent_usd"] <= float(cap)
    return [
        Check(
            "matrix.schedule",
            bool(schedule) and scheduled == expected_cells and seed is not None and schedule == redrawn,
            (
                f"seed={seed} blocks={len(schedule)} cells={len(scheduled)}/{len(expected_cells)}"
                if schedule == redrawn
                else f"the recorded schedule is not what seed {seed} draws (hand-edited or a different harness)"
            )
            if schedule
            else "no randomized block schedule recorded in experiment.json",
        ),
        Check(
            "matrix.schedule_followed",
            followed is None,
            followed or f"{len(rows)} rows carry their block's schedule_index and are in the frozen order",
        ),
        Check(
            "matrix.environment",
            not env_missing,
            "python/platform, provider settings, attempt budget and image identity are frozen"
            if not env_missing
            else f"experiment.json records no {env_missing}",
        ),
        Check(
            "matrix.plans",
            plans_error is None and not unrecorded,
            plans_error
            or (
                f"a recorded planning outcome for every block ({len(plans)}; {len(failed_blocks)} without a usable plan)"
                if not unrecorded
                else f"blocks with no recorded planning outcome: {unrecorded}"
            ),
        ),
        Check(
            "matrix.paired_plans",
            not unpaired,
            "C and D executed the same recorded plan in every block"
            if not unpaired
            else f"{len(unpaired)} block(s) where C and D did not share one plan: {unpaired[:3]}",
        ),
        Check(
            "matrix.spend_cap",
            within_cap and not ledger["unpriced_operations"],
            f"${ledger['spent_usd']:.4f} billed over {ledger['operations']} operation(s) against a ${cap} ceiling"
            f" ({len(manifest.get('spend_cap_history') or [])} recorded setting(s))"
            if within_cap and not ledger["unpriced_operations"]
            else (
                "no aggregate spend ceiling recorded (ADR-010)"
                if not isinstance(cap, int | float) or cap <= 0
                else f"${ledger['spent_usd']:.4f} billed against a ${cap} ceiling, "
                f"{ledger['unpriced_operations']} operation(s) with unknown cost"
            ),
        ),
        Check(
            "matrix.frozen_effort",
            not anthropic or bool(effort),
            f"anthropic models {anthropic} ran at effort '{effort}'" if anthropic else "no Anthropic model in this experiment",
        ),
    ]


def _matrix_checks(benchmark_dir: Path) -> tuple[list[Check], dict[str, Any]]:
    """Recompute the M3 evidence from `experiment.json` + `runs.jsonl`; the summaries on disk must agree."""
    manifest = _read(benchmark_dir / "experiment.json")
    done = _read(benchmark_dir / "completion.json")
    spec = manifest.get("spec") if isinstance(manifest.get("spec"), dict) else {}
    model_specs = spec.get("models") if isinstance(spec.get("models"), dict) else {}
    suites = spec.get("suite_sha256") if isinstance(spec.get("suite_sha256"), dict) else {}
    experiment_id = str(manifest.get("experiment_id") or "")
    rows_path = benchmark_dir / "runs.jsonl"
    checks: list[Check] = []
    try:
        if not rows_path.is_file():
            raise ValueError(f"missing raw evidence: {rows_path}")
        rows = load_rows(rows_path, experiment_id)
        rows_error = None
    except ValueError as exc:
        rows, rows_error = [], str(exc)
    checks.append(Check("matrix.raw_rows", rows_error is None and bool(rows), rows_error or f"{len(rows)} rows in runs.jsonl"))
    unbound = [
        i
        for i, r in enumerate(rows, 1)
        if r.get("git_commit") != manifest.get("git_commit")
        or (r.get("n") is not None and suites.get(str(r.get("n"))) != r.get("suite_sha256"))
    ]
    checks.append(
        Check(
            "matrix.rows_bound",
            bool(rows) and not unbound,
            "every row carries the manifest's commit and suite identity"
            if not unbound
            else f"{len(unbound)} rows with a different commit/suite (first at line {unbound[0]})",
        )
    )
    configs, widths, repeats = list(spec.get("configs") or []), list(spec.get("widths") or []), int(spec.get("repeats") or 0)
    recomputed = completion(rows, configs=configs, widths=widths, repeats=repeats, require_priced=spec.get("mode") == "live")
    disagreements = [k for k in _RECOMPUTED_COMPLETION_FIELDS if done.get(k) != recomputed.get(k)]
    try:
        plans_path = benchmark_dir / "plans.jsonl"
        plans = load_plans(plans_path, experiment_id)
        plan_rows = load_rows(plans_path, experiment_id)
        plans_error = None
    except ValueError as exc:
        plans, plan_rows, plans_error = {}, [], str(exc)
    checks += _design_checks(
        spec, manifest, rows, plans, plans_error, configs=configs, widths=widths, repeats=repeats, plan_rows=plan_rows
    )
    checks += [
        Check("matrix.live", spec.get("mode") == "live", f"mode={spec.get('mode')}"),
        Check("matrix.configs", set(configs) == CONFIGS, f"configs={configs}"),
        Check("matrix.widths", set(widths) == WIDTHS, f"widths={widths}"),
        Check("matrix.repetitions", repeats >= 5, f"repeats={repeats}"),
        Check(
            "matrix.models",
            all(model_specs.get(k) for k in ("cheap", "strong", "planner", "worker")),
            f"models={model_specs}",
        ),
        Check("matrix.prices", bool(spec.get("model_prices")), "a frozen price snapshot is required"),
        Check("matrix.clean_git", manifest.get("git_dirty") is False, f"commit={manifest.get('git_commit')}"),
        Check(
            "matrix.complete",
            recomputed["evidence_complete"] and recomputed["expected_runs"] >= 100,
            f"recomputed from raw rows: {recomputed['effective_runs']}/{recomputed['expected_runs']} effective, "
            f"missing={len(recomputed['missing'])} duplicates={recomputed['duplicate_rows']} "
            f"infrastructure_invalid={recomputed['infrastructure_invalid_rows']} unpriced={recomputed['unpriced_rows']}",
        ),
        Check("matrix.priced", recomputed["unpriced_rows"] == 0, f"unpriced={recomputed['unpriced_rows']} (recomputed)"),
        Check(
            "matrix.no_infrastructure_invalid",
            recomputed["infrastructure_invalid_rows"] == 0,
            f"cells still needing a rerun={recomputed['infrastructure_invalid_rows']}",
        ),
        Check(
            "matrix.completion_json",
            not disagreements,
            "completion.json agrees with the recomputation" if not disagreements else f"stale/forged fields: {disagreements}",
        ),
    ]
    analysis_path = benchmark_dir / "analysis.md"
    expected_report = None
    if rows and spec.get("widths") and spec.get("configs"):
        stamped = dict(recomputed)
        stamped.update({k: done.get(k) for k in ("schema", "experiment_id", "updated_at") if k in done})
        expected_report = render_analysis(aggregate(rows), manifest, stamped, plans)
    on_disk = analysis_path.read_text(encoding="utf-8") if analysis_path.is_file() else None
    if on_disk is None:
        detail = "analysis.md missing"
    elif expected_report is not None and on_disk == expected_report:
        detail = "analysis.md matches the report regenerated from the raw rows"
    else:
        detail = "analysis.md differs from the regenerated report (stale or edited)"
    checks.append(Check("matrix.analysis", expected_report is not None and on_disk == expected_report, detail))
    return checks, manifest


def evaluate(
    live_path: Path, distributed_path: Path, benchmark_dir: Path, *, current_git: tuple[str, bool] | None = None
) -> list[Check]:
    live = _read(live_path)
    distributed = _read(distributed_path)
    matrix_checks, manifest = _matrix_checks(benchmark_dir)
    revision, dirty = current_git if current_git is not None else current_git_state()
    live_git = live.get("git") if isinstance(live.get("git"), dict) else {}
    distributed_git = distributed.get("git") if isinstance(distributed.get("git"), dict) else {}
    distributed_run = distributed.get("run") if isinstance(distributed.get("run"), dict) else {}
    runs = live.get("runs") if isinstance(live.get("runs"), list) else []
    steps = live.get("steps") if isinstance(live.get("steps"), dict) else {}
    ping = live.get("ping") if isinstance(live.get("ping"), dict) else None
    checks = [
        Check("live.complete", live.get("complete") is True, f"steps={steps}"),
        Check(
            "live.four_stages",
            all(steps.get(k) is True for k in ("ping", "worker", "planner", "repair")),
            "requires ping, worker, planner and repair",
        ),
        Check(
            "live.runs",
            len(runs) == 3 and all(isinstance(r, dict) and r.get("status") == "PASSED" for r in runs),
            f"recorded={len(runs)} (worker, planner, repair)",
        ),
        Check(
            "live.priced",
            bool(runs)
            and all(isinstance(r, dict) and r.get("priced") is True for r in runs)
            # The ping is one call, but an unpriced one means the price table does not cover the model the provider
            # reported — every cost figure after it is then a floor (ADR-010). Its telemetry must be *present*: a
            # `steps.ping = true` with no record is a claim with no evidence behind it, which is what this gate exists
            # to refuse.
            and isinstance(ping, dict)
            and bool(ping.get("calls"))
            and ping.get("priced") is True,
            "every live run must have known model prices, and the ping must be on record and priced"
            + ("" if isinstance(ping, dict) else " (no ping telemetry in the evidence)"),
        ),
        Check(
            "live.manual_contract_approval",
            live.get("manual_contract_approval") is True,
            "run live_smoke with --no-auto-approve",
        ),
        Check("live.clean_git", live_git.get("dirty") is False, f"commit={live_git.get('commit')}"),
        Check(
            "distributed.complete",
            distributed.get("complete") is True and distributed.get("mode") == "live",
            f"mode={distributed.get('mode')}",
        ),
        Check(
            "distributed.run",
            distributed_run.get("status") == "PASSED" and distributed.get("priced") is True,
            f"status={distributed_run.get('status')} priced={distributed.get('priced')}",
        ),
        Check(
            "distributed.clean_git",
            distributed_git.get("dirty") is False,
            f"commit={distributed_git.get('commit')}",
        ),
        *matrix_checks,
        Check(
            "same_revision",
            bool(live_git.get("commit"))
            and live_git.get("commit") == distributed_git.get("commit") == manifest.get("git_commit"),
            f"live={live_git.get('commit')} distributed={distributed_git.get('commit')} matrix={manifest.get('git_commit')}",
        ),
        Check(
            "current.revision",
            bool(revision) and revision != "unknown" and revision == manifest.get("git_commit"),
            f"checked out={revision} evidence={manifest.get('git_commit')}",
        ),
        Check("current.clean", dirty is False, "working tree is clean" if not dirty else "working tree has uncommitted changes"),
    ]
    return checks


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--live", type=Path, default=ROOT / "mvp-evidence" / "live-smoke.json")
    ap.add_argument("--distributed", type=Path, default=ROOT / "mvp-evidence" / "distributed-smoke.json")
    ap.add_argument("--benchmark", type=Path, default=ROOT / "benchmark-results")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)
    try:
        checks = evaluate(args.live, args.distributed, args.benchmark)
    except ValueError as exc:
        if args.json:
            print(json.dumps({"ok": False, "error": str(exc)}, indent=2))
        else:
            print(f"MVP EVIDENCE: INCOMPLETE\nFAIL evidence {exc}")
        return 2
    ok = all(c.ok for c in checks)
    if args.json:
        print(json.dumps({"ok": ok, "checks": [asdict(c) for c in checks]}, indent=2))
    else:
        print("MVP EVIDENCE: " + ("COMPLETE" if ok else "INCOMPLETE"))
        for c in checks:
            print(f"{'PASS' if c.ok else 'FAIL':4s} {c.name:34s} {c.detail}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
