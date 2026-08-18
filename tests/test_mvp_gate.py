"""The MVP evidence gate and the M3 runner's evidence integrity: raw rows are the truth, summaries must agree, evidence
binds to one clean commit, infrastructure failures are rerun rather than counted, and the report is regenerable."""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import benchmark, distributed_smoke, live_smoke
from scripts.benchmark import classify_run, completion, crossover, effective_rows
from scripts.mvp_gate import evaluate

ROOT = Path(__file__).resolve().parents[1]
CONFIGS = list("ABCD")
WIDTHS = [1, 2, 4, 8, 16]
COMMIT = "abc"
SUITES = {str(n): f"suite{n}" for n in WIDTHS}
EXPERIMENT = "exp-1"
SEED = 7
SPEND_CAP = 500.0
SCHEDULE = benchmark.build_schedule(CONFIGS, WIDTHS, 5, SEED)
BLOCK_INDEX = {(int(b["n"]), int(b["repetition"])): int(b["index"]) for b in SCHEDULE}


def _plan_sha(n: int, repetition: int) -> str:
    return f"plan-{n}-{repetition}"


def _plan(n: int, repetition: int, **extra) -> dict:
    """A block's shared plan record, as `benchmark.make_plan` writes it to plans.jsonl (ADR-009)."""
    record = {
        "experiment_id": EXPERIMENT,
        "git_commit": COMMIT,
        "n": n,
        "repetition": repetition,
        "source": "planner",
        "planned": True,
        "outcome": "planned",
        "plan_sha256": _plan_sha(n, repetition),
        "tasks": n + 1,
        "cost_known": True,
        "call_cost_usd": 0.02,
        "model_calls": 1,
        "plan_rejections": 0,
        "plan_s": 3.0,
    }
    record.update(extra)
    return record


def _plans() -> list[dict]:
    return [_plan(n, r) for n in WIDTHS for r in range(1, 6)]


def _row(config: str, n: int, repetition: int, *, status: str = "PASSED", priced: bool = True, **extra) -> dict:
    """A row shaped like `benchmark.run_one` + the manifest stamps."""
    row = {
        "config": config,
        "n": n,
        "repetition": repetition,
        # C and D replay the block's one shared plan; A/B are single-agent and have none (ADR-009)
        "plan_sha256": _plan_sha(n, repetition) if config in benchmark.REPLAY_CONFIGS else None,
        "status": status,
        "verdict": "PASS" if status == "PASSED" else f"FAIL:{extra.pop('reason_text', 'verification failed')}",
        "verdict_reason": None if status == "PASSED" else extra.pop("verdict_reason", "NO_PROGRESS"),
        "cost_known": priced,
        "run_id": f"run-{config}-{n}-{repetition}-{extra.pop('suffix', 0)}",
        "machine_s": 10.0 * n,
        "wall_clock_s": 8.0 * n,
        "human_wait_s": 0.0,
        "critical_path_s": 4.0 * n,
        "parallelism_efficiency": 1.5,
        "worker_utilisation": 0.7,
        "call_cost_usd": 0.01 * n if priced else None,
        "call_input_tokens": 1000 * n,
        "call_output_tokens": 200 * n,
        "call_cache_read_tokens": 0,
        "call_seconds": 4.0,
        "model_calls": 4,
        "tasks": n + 1,
        "attempts": n + 1,
        "retries": 0,
        "abandoned": 0,
        "replans_used": 0,
        "plan_rejections": 0,
        "questions": 0,
        "assumptions": 1,
        "verifier_fails": 0 if status == "PASSED" else 1,
        "attempt_failure_classes": {},
        "task_shape": {"estimated_width": n, "suggested_mode": "parallel_centralized_mas"},
        "experiment_id": EXPERIMENT,
        "git_commit": COMMIT,
        "suite_sha256": SUITES[str(n)],
        "schedule_index": BLOCK_INDEX[(n, repetition)],
    }
    row.update(extra)
    return row


def _rows(*, priced: bool = True, status: str = "PASSED") -> list[dict]:
    """In the frozen schedule's own order — that is how the harness appends them, and the gate audits it."""
    return [_row(c, n, r, status=status, priced=priced) for c, n, r in benchmark.schedule_cells(SCHEDULE)]


def _replace(rows: list[dict], row: dict) -> list[dict]:
    """Swap the row for one cell, keeping the recorded order (positions are schedule order, not config order)."""
    key = (row["config"], row["n"], row["repetition"])
    return [row if (r["config"], r["n"], r["repetition"]) == key else r for r in rows]


ENVIRONMENT = {
    "python": "3.12.4",
    "python_implementation": "CPython",
    "platform": "Windows-11",
    "provider": {"timeout_s": 600.0, "max_retries": 2, "anthropic_thinking": True},
    "attempt": {"max_calls": 40, "max_tokens": None},
    "exec": {"image": "mas-verifier:latest", "image_id": "sha256:aaa", "cpus": 1.0},
    "verifier": {"image": "mas-verifier:latest", "image_id": "sha256:aaa", "timeout_s": 300},
    "worker_capabilities": ["implementation"],
}


def _manifest() -> dict:
    return {
        "schema": 3,
        "experiment_id": EXPERIMENT,
        "git_commit": COMMIT,
        "git_dirty": False,
        "spend_cap_usd": SPEND_CAP,
        "spend_cap_history": [{"at": "t0", "git_commit": COMMIT, "from_usd": None, "to_usd": SPEND_CAP}],
        "spec": {
            "mode": "live",
            "configs": CONFIGS,
            "widths": WIDTHS,
            "repeats": 5,
            "seed": SEED,
            "models": {"cheap": "a", "strong": "b", "planner": "c", "worker": "d"},
            "budgets": {"max_tokens": 1, "max_attempt_tokens": 1, "max_cost_usd": 1.0, "max_wallclock_s": 1, "max_replans": 1},
            "model_prices": {"a": {"input": 1, "output": 2}},
            "suite_sha256": SUITES,
            "environment": ENVIRONMENT,
        },
        "schedule": SCHEDULE,
    }


def _write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _write_matrix(bench: Path, rows: list[dict], *, manifest: dict | None = None, plans: list[dict] | None = None) -> dict:
    """Exactly what `benchmark.py` leaves behind: manifest, raw rows, block plans, recomputed completion, report."""
    manifest = manifest or _manifest()
    _write(bench / "experiment.json", manifest)
    bench.mkdir(parents=True, exist_ok=True)
    (bench / "runs.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    plan_records = _plans() if plans is None else plans
    (bench / "plans.jsonl").write_text("".join(json.dumps(p) + "\n" for p in plan_records), encoding="utf-8")
    spec = manifest["spec"]
    done = completion(rows, configs=spec["configs"], widths=spec["widths"], repeats=spec["repeats"], require_priced=True)
    done.update({"schema": 1, "experiment_id": EXPERIMENT, "updated_at": "t"})
    _write(bench / "completion.json", done)
    loaded = benchmark.load_plans(bench / "plans.jsonl", EXPERIMENT)
    benchmark.write_analysis(benchmark.aggregate(rows), manifest, done, loaded, bench / "analysis.md")
    return done


def _write_live_and_distributed(live: Path, distributed: Path) -> None:
    _write(
        live,
        {
            "complete": True,
            "steps": {"ping": True, "worker": True, "planner": True, "repair": True},
            "runs": [
                {"step": "worker", "status": "PASSED", "priced": True},
                {"step": "planner", "status": "PASSED", "priced": True},
                {"step": "repair", "status": "PASSED", "priced": True},
            ],
            "manual_contract_approval": True,
            "git": {"commit": COMMIT, "dirty": False},
        },
    )
    _write(
        distributed,
        {
            "complete": True,
            "mode": "live",
            "priced": True,
            "run": {"status": "PASSED"},
            "git": {"commit": COMMIT, "dirty": False},
        },
    )


def _failed(checks) -> set[str]:
    return {c.name for c in checks if not c.ok}


# ----------------------------------------------------------------------------- failure classification


def _failed_row(verdict: str, reason: str, status: str = "FAILED", **extra) -> dict:
    return {"status": status, "verdict": verdict, "verdict_reason": reason, **extra}


UNRECOVERABLE = "UNRECOVERABLE_FAILURE"
RETRIES = "FAIL:task T2 failed (retries exhausted)"


@pytest.mark.parametrize(
    "row, expected",
    [
        ({"status": "PASSED"}, "pass"),
        ({"status": "CLIENT_ERROR"}, "infrastructure"),
        ({"status": "RUNNING"}, "infrastructure"),
        (_failed_row("FAIL:verification failed: 2 checks", "NO_PROGRESS"), "experimental"),
        (_failed_row("FAIL:verification failed", "BUDGET_EXHAUSTED"), "experimental"),
        (_failed_row("FAIL:planner could not produce a valid DAG", "INVALID_PLAN"), "experimental"),
        (_failed_row("ABORTED:token budget exhausted", "BUDGET_EXHAUSTED", status="ABORTED"), "experimental"),
        (_failed_row("ABORTED:operator", "POLICY_DENIED", status="ABORTED"), "infrastructure"),
        (_failed_row("FAIL:verification not completed (ERROR): sandbox died", UNRECOVERABLE), "infrastructure"),
        (_failed_row("FAIL:verification not completed (INVALID): bad suite", "UNSUPPORTED"), "infrastructure"),
        # UNSUPPORTED from the planner (a contract that maps to no trusted adapter, ADR-008 §6) is the model's outcome
        (_failed_row("FAIL:planner rejected: unmappable acceptance criteria: check[0].type 'x'", "UNSUPPORTED"), "experimental"),
        (
            _failed_row("FAIL:verification failed; repair needs a planner and none is configured (--planner)", UNRECOVERABLE),
            "infrastructure",
        ),
        # a task exhausted its attempts: infrastructure only when *every* failed attempt was the machinery's
        (_failed_row(RETRIES, UNRECOVERABLE, attempt_failure_classes={"infrastructure": 3}), "infrastructure"),
        (_failed_row(RETRIES, UNRECOVERABLE, attempt_failure_classes={"infrastructure": 2, "model": 1}), "experimental"),
        (_failed_row(RETRIES, UNRECOVERABLE, attempt_failure_classes={"model": 3, "abandoned": 1}), "experimental"),
        # worker death is never the model's doing: attempts that only ever ended abandoned/cancelled exonerate it
        (_failed_row(RETRIES, UNRECOVERABLE, attempt_failure_classes={"abandoned": 3}), "infrastructure"),
        (_failed_row(RETRIES, UNRECOVERABLE, attempt_failure_classes={"abandoned": 2, "infrastructure": 1}), "infrastructure"),
        (_failed_row(RETRIES, UNRECOVERABLE, attempt_failure_classes={"abandoned": 2, "budget": 1}), "experimental"),
        # no recorded classes: nothing exonerates the model, the run stays evidence
        (_failed_row(RETRIES, UNRECOVERABLE), "experimental"),
        (_failed_row(RETRIES, UNRECOVERABLE, attempt_failure_classes={}), "experimental"),
    ],
)
def test_run_failure_classification_is_deterministic(row: dict, expected: str):
    assert classify_run(row) == expected


def test_effective_rows_let_a_rerun_supersede_only_infrastructure_invalid_rows():
    infra = _row("A", 1, 1, status="FAILED", verdict_reason="UNSUPPORTED", reason_text="verification not completed (INVALID)")
    rerun = _row("A", 1, 1, suffix=1)
    effective, audit = effective_rows([infra, rerun])
    assert effective[("A", 1, 1)] is rerun and audit == {"superseded": 1, "duplicates": []}

    valid_then_again = [_row("A", 1, 1), _row("A", 1, 1, suffix=1)]
    _, audit = effective_rows(valid_then_again)
    assert audit["duplicates"] == [("A", 1, 1)] and audit["superseded"] == 0


def test_matrix_completion_accepts_experimental_failures_but_not_missing_duplicate_infrastructure_or_unpriced_rows():
    kwargs = dict(configs=CONFIGS, widths=WIDTHS, repeats=5, require_priced=True)
    rows = _rows(status="FAILED")
    done = completion(rows, **kwargs)
    assert done["evidence_complete"] and not done["all_passed"] and done["expected_runs"] == 100
    assert done["failure_classes"] == {"pass": 0, "experimental": 100, "infrastructure": 0}

    assert not completion(rows[:-1], **kwargs)["evidence_complete"]  # missing
    assert not completion(_rows(priced=False), **kwargs)["evidence_complete"]  # unpriced
    assert not completion(rows + [rows[0]], **kwargs)["evidence_complete"]  # duplicate of a valid row

    infra = _row("B", 4, 2, status="FAILED", verdict_reason="UNSUPPORTED", reason_text="verification not completed (INVALID): x")
    with_infra = [infra if (r["config"], r["n"], r["repetition"]) == ("B", 4, 2) else r for r in _rows()]
    done = completion(with_infra, **kwargs)
    assert not done["evidence_complete"]
    assert done["infrastructure_invalid_rows"] == 1 and done["keys_needing_rerun"] == [{"config": "B", "n": 4, "repetition": 2}]

    rerun = _row("B", 4, 2, suffix=1)
    done = completion(with_infra + [rerun], **kwargs)
    assert done["evidence_complete"] and done["all_passed"]
    assert done["superseded_rows"] == 1 and done["duplicate_rows"] == 0 and done["recorded_runs"] == 101


def test_aggregate_reports_distributions_over_evidence_runs_and_excludes_infrastructure_rows():
    rows = [_row("D", 4, r, machine_s=float(r)) for r in range(1, 6)]
    invalid = _row(
        "D", 4, 5, status="FAILED", verdict_reason="UNSUPPORTED", reason_text="verification not completed (INVALID)", suffix=1
    )
    rows.append(invalid)
    # the infra row came last for repetition 5, so it is the effective row of that key -> excluded from evidence
    (record,) = benchmark.aggregate(rows)
    assert record["runs"] == 5 and record["evidence_runs"] == 4 and record["infrastructure_invalid"] == 1
    assert record["success_rate"] == 1.0
    dist = record["distributions"]["machine_s"]
    assert dist == {"n": 4, "min": 1.0, "p25": 1.75, "median": 2.5, "p75": 3.25, "max": 4.0, "mean": 2.5}
    assert record["distributions"]["tokens_in_per_attempt"]["median"] == pytest.approx(4000 / 5)
    assert record["distributions"]["call_latency_s"]["median"] == pytest.approx(1.0)
    assert record["suggested_modes"] == {"parallel_centralized_mas": 4} and record["estimated_widths"] == {"4": 4}
    flat = benchmark.scalar_summary([record])[0]
    assert "distributions" not in flat and flat["median_critical_path_s"] == 16.0


# ----------------------------------------------------------------------------- the gate


def test_mvp_evidence_passes_only_when_raw_rows_summaries_and_current_checkout_agree(tmp_path: Path):
    live, distributed, bench = tmp_path / "live.json", tmp_path / "distributed.json", tmp_path / "bench"
    _write_live_and_distributed(live, distributed)
    _write_matrix(bench, _rows())
    checks = evaluate(live, distributed, bench, current_git=(COMMIT, False))
    assert checks and all(c.ok for c in checks), _failed(checks)

    # a stage that did not pass
    broken = json.loads(live.read_text(encoding="utf-8"))
    broken["steps"]["repair"] = False
    _write(live, broken)
    assert "live.four_stages" in _failed(evaluate(live, distributed, bench, current_git=(COMMIT, False)))
    _write_live_and_distributed(live, distributed)

    # bound to the current checkout: a different HEAD or a dirty tree fails even though the evidence agrees with itself
    assert _failed(evaluate(live, distributed, bench, current_git=("later", False))) == {"current.revision"}
    assert _failed(evaluate(live, distributed, bench, current_git=(COMMIT, True))) == {"current.clean"}


def test_mvp_gate_refuses_fabricated_or_stale_summaries_without_matching_raw_rows(tmp_path: Path):
    live, distributed, bench = tmp_path / "live.json", tmp_path / "distributed.json", tmp_path / "bench"
    _write_live_and_distributed(live, distributed)
    # the review's forgery: a manifest, a completion.json claiming success and a one-line analysis.md, no runs.jsonl
    _write(bench / "experiment.json", _manifest())
    _write(bench / "completion.json", {"evidence_complete": True, "expected_runs": 100, "recorded_runs": 100, "unpriced_rows": 0})
    (bench / "analysis.md").write_text("# result\n", encoding="utf-8")
    failed = _failed(evaluate(live, distributed, bench, current_git=(COMMIT, False)))
    assert {"matrix.raw_rows", "matrix.complete", "matrix.completion_json", "matrix.analysis"} <= failed

    # real rows, but a stale completion.json (written before the last rows landed) and an edited report
    rows = _rows()
    _write_matrix(bench, rows[:-3])
    (bench / "runs.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    failed = _failed(evaluate(live, distributed, bench, current_git=(COMMIT, False)))
    assert "matrix.completion_json" in failed and "matrix.analysis" in failed and "matrix.complete" not in failed

    _write_matrix(bench, rows)
    (bench / "analysis.md").write_text((bench / "analysis.md").read_text(encoding="utf-8") + "\nedited\n", encoding="utf-8")
    assert _failed(evaluate(live, distributed, bench, current_git=(COMMIT, False))) == {"matrix.analysis"}


def test_mvp_gate_rejects_rows_from_another_commit_or_suite_and_infrastructure_invalid_cells(tmp_path: Path):
    live, distributed, bench = tmp_path / "live.json", tmp_path / "distributed.json", tmp_path / "bench"
    _write_live_and_distributed(live, distributed)
    rows = _rows()
    rows[7]["git_commit"] = "other"
    rows[8]["suite_sha256"] = "tampered"
    _write_matrix(bench, rows)
    assert _failed(evaluate(live, distributed, bench, current_git=(COMMIT, False))) == {"matrix.rows_bound"}

    rows = _replace(
        _rows(),
        _row("A", 1, 4, status="FAILED", verdict_reason="UNSUPPORTED", reason_text="verification not completed (INVALID)"),
    )
    _write_matrix(bench, rows)
    failed = _failed(evaluate(live, distributed, bench, current_git=(COMMIT, False)))
    assert failed == {"matrix.complete", "matrix.no_infrastructure_invalid"}
    # ...and passes again once the cell has been rerun (append-only: the invalid row stays on record). A rerun is
    # appended out of schedule order by construction, which is why it carries `rerun_of` and is exempt from the audit.
    _write_matrix(bench, rows + [_row("A", 1, 4, suffix=1, rerun_of="run-A-1-4-0", rerun_index=1)])
    assert not _failed(evaluate(live, distributed, bench, current_git=(COMMIT, False)))


def test_mvp_gate_requires_c_and_d_to_have_executed_one_shared_plan_per_block(tmp_path: Path):
    """ADR-009: without a shared plan, a C-versus-D difference measures a different plan *and* different concurrency."""
    live, distributed, bench = tmp_path / "live.json", tmp_path / "distributed.json", tmp_path / "bench"
    _write_live_and_distributed(live, distributed)

    # each configuration planned for itself (the defect the reviewer found): C and D of one block disagree
    rows = _rows()
    for row in rows:
        if (row["config"], row["n"], row["repetition"]) == ("D", 4, 2):
            row["plan_sha256"] = "a-plan-of-its-own"
    _write_matrix(bench, rows)
    assert _failed(evaluate(live, distributed, bench, current_git=(COMMIT, False))) == {"matrix.paired_plans"}

    # rows that carry no plan identity at all (an older harness) are just as unacceptable
    rows = _rows()
    for row in rows:
        row.pop("plan_sha256", None)
    _write_matrix(bench, rows)
    assert "matrix.paired_plans" in _failed(evaluate(live, distributed, bench, current_git=(COMMIT, False)))

    # a block whose plan was never recorded: the rows claim a plan that is not on file
    _write_matrix(bench, _rows(), plans=[p for p in _plans() if (p["n"], p["repetition"]) != (8, 3)])
    failed = _failed(evaluate(live, distributed, bench, current_git=(COMMIT, False)))
    assert failed == {"matrix.plans", "matrix.paired_plans"}


def test_a_block_the_planner_could_not_plan_is_evidence_not_a_reason_to_re_roll_the_planner(tmp_path: Path):
    """The planner failing for one block is an experimental result for C and D. Requiring a *successful* plan per
    block would push the operator to rerun until the planner cooperated — precisely the bias this gate prevents."""
    live, distributed, bench = tmp_path / "live.json", tmp_path / "distributed.json", tmp_path / "bench"
    _write_live_and_distributed(live, distributed)
    dead = _plan(8, 3, planned=False, outcome="failed", plan_sha256=None, tasks=None, verdict_reason="INVALID_PLAN")
    plans = [p for p in _plans() if (p["n"], p["repetition"]) != (8, 3)] + [dead]
    stamps = {
        "experiment_id": EXPERIMENT,
        "git_commit": COMMIT,
        "suite_sha256": SUITES["8"],
        "schedule_index": BLOCK_INDEX[(8, 3)],
    }
    rows = [
        benchmark.plan_failure_row(r["config"], 8, 3, {"status": "FAILED", "verdict": "no plan", **dead}) | stamps
        if (r["config"], r["n"], r["repetition"]) in {("C", 8, 3), ("D", 8, 3)}
        else r
        for r in _rows()
    ]
    _write_matrix(bench, rows, plans=plans)
    assert not _failed(evaluate(live, distributed, bench, current_git=(COMMIT, False)))

    # ...but D quietly executing *something* for a block that has no shared plan is not acceptable
    improvised = [_row("D", 8, 3) if (r["config"], r["n"], r["repetition"]) == ("D", 8, 3) else r for r in rows]
    _write_matrix(bench, improvised, plans=plans)
    assert "matrix.paired_plans" in _failed(evaluate(live, distributed, bench, current_git=(COMMIT, False)))


def test_mvp_gate_requires_a_frozen_schedule_and_environment(tmp_path: Path):
    live, distributed, bench = tmp_path / "live.json", tmp_path / "distributed.json", tmp_path / "bench"
    _write_live_and_distributed(live, distributed)

    manifest = _manifest()
    del manifest["schedule"]
    _write_matrix(bench, _rows(), manifest=manifest)
    assert "matrix.schedule" in _failed(evaluate(live, distributed, bench, current_git=(COMMIT, False)))

    manifest = _manifest()  # a schedule that does not cover every cell is not the experiment that ran
    manifest["schedule"] = manifest["schedule"][:-1]
    _write_matrix(bench, _rows(), manifest=manifest)
    assert "matrix.schedule" in _failed(evaluate(live, distributed, bench, current_git=(COMMIT, False)))

    manifest = _manifest()
    del manifest["spec"]["environment"]
    _write_matrix(bench, _rows(), manifest=manifest)
    assert "matrix.environment" in _failed(evaluate(live, distributed, bench, current_git=(COMMIT, False)))

    # a schedule that covers every cell but is not what the frozen seed draws: hand-edited, or another harness
    manifest = _manifest()
    manifest["schedule"] = [dict(b, configs=sorted(b["configs"])) for b in manifest["schedule"]]
    _write_matrix(bench, _rows(), manifest=manifest)
    assert "matrix.schedule" in _failed(evaluate(live, distributed, bench, current_git=(COMMIT, False)))


def test_mvp_gate_requires_the_evidence_to_have_been_produced_in_the_frozen_order(tmp_path: Path):
    """A recorded schedule nobody followed is decoration (ADR-010)."""
    live, distributed, bench = tmp_path / "live.json", tmp_path / "distributed.json", tmp_path / "bench"
    _write_live_and_distributed(live, distributed)
    _write_matrix(bench, _rows())
    assert not _failed(evaluate(live, distributed, bench, current_git=(COMMIT, False)))

    # rows produced config-major (the pre-ADR-009 order), while the manifest claims a randomized schedule
    ordered = sorted(_rows(), key=lambda r: (r["config"], r["n"], r["repetition"]))
    _write_matrix(bench, ordered)
    assert "matrix.schedule_followed" in _failed(evaluate(live, distributed, bench, current_git=(COMMIT, False)))

    # ...and a row that does not carry its own block's index is just as unacceptable
    rows = _replace(_rows(), _row("D", 8, 3, schedule_index=BLOCK_INDEX[(8, 3)] + 1))
    _write_matrix(bench, rows)
    assert "matrix.schedule_followed" in _failed(evaluate(live, distributed, bench, current_git=(COMMIT, False)))


def test_mvp_gate_requires_a_recorded_spend_ceiling_the_recomputed_spend_stayed_inside(tmp_path: Path):
    live, distributed, bench = tmp_path / "live.json", tmp_path / "distributed.json", tmp_path / "bench"
    _write_live_and_distributed(live, distributed)

    manifest = _manifest()
    del manifest["spend_cap_usd"]
    _write_matrix(bench, _rows(), manifest=manifest)
    assert "matrix.spend_cap" in _failed(evaluate(live, distributed, bench, current_git=(COMMIT, False)))

    manifest = _manifest()  # a ceiling the evidence itself shows was blown through
    manifest["spend_cap_usd"] = 0.5
    _write_matrix(bench, _rows(), manifest=manifest)
    assert "matrix.spend_cap" in _failed(evaluate(live, distributed, bench, current_git=(COMMIT, False)))


def test_mvp_gate_requires_reasoning_effort_to_have_been_an_explicit_frozen_choice(tmp_path: Path):
    """An unset effort means the provider's default decided cost and behaviour, on a run whose manifest claims to
    freeze exactly that."""
    live, distributed, bench = tmp_path / "live.json", tmp_path / "distributed.json", tmp_path / "bench"
    _write_live_and_distributed(live, distributed)
    anthropic = {"cheap": "anthropic:a", "strong": "anthropic:b", "planner": "anthropic:c", "worker": "anthropic:d"}

    manifest = _manifest()
    manifest["spec"]["models"] = anthropic
    _write_matrix(bench, _rows(), manifest=manifest)
    assert "matrix.frozen_effort" in _failed(evaluate(live, distributed, bench, current_git=(COMMIT, False)))

    manifest = _manifest()
    manifest["spec"]["models"] = anthropic
    manifest["spec"]["environment"] = {**ENVIRONMENT, "provider": {**ENVIRONMENT["provider"], "anthropic_effort": "medium"}}
    _write_matrix(bench, _rows(), manifest=manifest)
    assert not _failed(evaluate(live, distributed, bench, current_git=(COMMIT, False)))


def test_schedule_is_deterministic_covers_every_cell_and_interleaves_configurations():
    schedule = benchmark.build_schedule(CONFIGS, WIDTHS, 5, SEED)
    assert schedule == benchmark.build_schedule(CONFIGS, WIDTHS, 5, SEED)  # same seed, same experiment
    assert schedule != benchmark.build_schedule(CONFIGS, WIDTHS, 5, SEED + 1)
    cells = benchmark.schedule_cells(schedule)
    assert sorted(cells) == sorted((c, n, r) for c in CONFIGS for n in WIDTHS for r in range(1, 6))
    assert len(cells) == len(set(cells)) == 100
    # every block holds all four configurations (interleaved), and the blocks themselves are not in width order
    assert all(sorted(b["configs"]) == sorted(CONFIGS) for b in schedule)
    assert [b["n"] for b in schedule] != sorted(b["n"] for b in schedule)
    assert len({tuple(b["configs"]) for b in schedule}) > 1  # not one fixed order for every block


def test_c_and_d_commands_differ_only_in_concurrency_and_replay_the_same_plan(tmp_path: Path):
    args = argparse.Namespace(
        offline=False,
        cheap_model="p:cheap",
        strong_model="p:strong",
        planner_model="p:planner",
        worker_model="p:worker",
        max_tokens=1,
        max_attempt_tokens=2,
        max_cost_usd=3.0,
        max_wallclock_s=4,
        max_replans=1,
    )
    plan, fixture = tmp_path / "n04-r1.json", tmp_path / "adapters_4.json"
    c = benchmark.command(args, "C", 4, fixture, plan)
    d = benchmark.command(args, "D", 4, fixture, plan)
    assert c.count(str(plan)) == 1 and d.count(str(plan)) == 1  # both execute the block's plan, neither re-plans
    assert "--goal" not in c and "--goal" not in d
    assert c[c.index("--workers") :] == ["--workers", "1", "--max-concurrency", "1"]
    assert d[d.index("--workers") :] == ["--workers", "4", "--max-concurrency", "4"]
    # everything else — models, suite, budgets, backend, the plan file — is identical: concurrency is the only knob
    knobs = {"--config", "--workers", "--max-concurrency"}
    strip = lambda argv: [a for i, a in enumerate(argv) if a not in knobs and (i == 0 or argv[i - 1] not in knobs)]  # noqa: E731
    assert strip(c) == strip(d)
    with pytest.raises(ValueError, match="shared plan"):
        benchmark.command(args, "D", 4, fixture, None)


def test_make_plan_records_what_mas_plan_reported_and_verifies_the_exported_file(tmp_path: Path, monkeypatch):
    """The live path, without a provider: `mas plan --json` is the source of truth and the file must match its digest."""
    args = argparse.Namespace(
        offline=False,
        planner_model="p:planner",
        max_tokens=1,
        max_attempt_tokens=2,
        max_cost_usd=3.0,
        max_wallclock_s=4,
        max_replans=1,
    )
    dag = {"goal": "g", "tasks": [{"id": "T1", "capability": "implementation", "goal": "x"}]}
    digest = benchmark.plan_digest(dag)
    path = tmp_path / "n02-r1.json"

    def fake_plan(argv, **kw):
        assert "plan" in argv and "--max-concurrency" in argv and argv[argv.index("--max-concurrency") + 1] == "2"
        path.write_text(json.dumps(dag), encoding="utf-8")
        record = {"run_id": "r1", "planned": True, "plan_sha256": digest, "tasks": 1, "call_cost_usd": 0.4, "output": "x"}
        return subprocess.CompletedProcess(argv, 0, json.dumps(record), "")

    monkeypatch.setattr(benchmark.subprocess, "run", fake_plan)
    record = benchmark.make_plan(args, 2, 1, path)
    assert record["planned"] and record["outcome"] == "planned" and record["plan_sha256"] == digest
    assert record["dag"] == dag and record["tasks"] == 1 and record["call_cost_usd"] == 0.4
    assert "output" not in record  # the caller's path, not evidence

    # the planner asked instead of planning, and a file that does not match the reported plan
    def parked(argv, **kw):
        return subprocess.CompletedProcess(argv, 2, json.dumps({"planned": False, "parked": True}), "")

    monkeypatch.setattr(benchmark.subprocess, "run", parked)
    assert benchmark.make_plan(args, 2, 1, path)["outcome"] == "questions"

    def tampered(argv, **kw):
        path.write_text(json.dumps({"goal": "something else", "tasks": []}), encoding="utf-8")
        return subprocess.CompletedProcess(argv, 0, json.dumps({"planned": True, "plan_sha256": digest}), "")

    monkeypatch.setattr(benchmark.subprocess, "run", tampered)
    tainted = benchmark.make_plan(args, 2, 1, path)
    assert not tainted["planned"] and tainted["outcome"] == "client_error"

    monkeypatch.setattr(benchmark.subprocess, "run", lambda argv, **kw: subprocess.CompletedProcess(argv, 1, "boom", ""))
    assert benchmark.make_plan(args, 2, 1, path)["outcome"] == "client_error"


def test_offline_block_plan_is_the_width_fixture_so_the_rehearsal_exercises_the_same_plumbing(tmp_path: Path):
    args = argparse.Namespace(offline=True)
    record = benchmark.make_plan(args, 4, 3, tmp_path / "n04-r3.json")
    assert record["source"] == "fixture" and record["planned"] and record["cost_known"]
    assert record["plan_sha256"] == benchmark.plan_digest(benchmark.width_dag(4).to_dict())
    assert json.loads((tmp_path / "n04-r3.json").read_text(encoding="utf-8"))["tasks"]


@pytest.mark.parametrize(
    "plan, expected",
    [
        ({"outcome": "questions", "verdict_reason": "CANCELLED"}, "experimental"),
        ({"outcome": "failed", "verdict_reason": "INVALID_PLAN"}, "experimental"),
        ({"outcome": "failed", "verdict_reason": "UNSUPPORTED"}, "experimental"),
        ({"outcome": "failed", "verdict_reason": "BUDGET_EXHAUSTED"}, "experimental"),
        ({"outcome": "failed", "verdict_reason": "UNRECOVERABLE_FAILURE"}, "infrastructure"),
        ({"outcome": "client_error", "verdict_reason": None}, "infrastructure"),
    ],
)
def test_a_block_without_a_plan_is_classified_by_why_the_planner_failed(plan: dict, expected: str):
    row = benchmark.plan_failure_row("C", 4, 2, {"status": "FAILED", "verdict": "no plan", **plan})
    assert row["plan_failed"] and row["config"] == "C"
    assert classify_run(row) == expected


def test_a_cancelled_run_is_never_evidence():
    """`CANCELLED` (ADR-009) means an operator ended it — e.g. a plan-only run. It says nothing about MAS."""
    assert classify_run({"status": "ABORTED", "verdict": "ABORTED:plan-only run", "verdict_reason": "CANCELLED"}) == (
        "infrastructure"
    )


def test_mvp_evidence_reports_missing_files(tmp_path: Path):
    with pytest.raises(ValueError, match="missing evidence"):
        evaluate(tmp_path / "missing.json", tmp_path / "distributed.json", tmp_path / "bench", current_git=(COMMIT, False))


def test_resumed_experiment_refuses_a_changed_revision_dirty_tree_or_environment(tmp_path: Path, monkeypatch):
    spec = {"configs": CONFIGS, "widths": WIDTHS, "repeats": 5, "seed": SEED, "environment": ENVIRONMENT}
    monkeypatch.setattr(benchmark, "_git_state", lambda: ("abc", False))
    manifest = benchmark.open_experiment(tmp_path, spec)
    assert manifest["git_commit"] == "abc" and not manifest["git_dirty"]
    assert benchmark.schedule_cells(manifest["schedule"]) == benchmark.schedule_cells(
        benchmark.build_schedule(CONFIGS, WIDTHS, 5, SEED)
    )
    assert benchmark.open_experiment(tmp_path, spec)["schedule"] == manifest["schedule"]  # replayed, never redrawn

    monkeypatch.setattr(benchmark, "_git_state", lambda: ("def", False))
    with pytest.raises(ValueError, match="current commit is def"):
        benchmark.open_experiment(tmp_path, spec)

    monkeypatch.setattr(benchmark, "_git_state", lambda: ("abc", True))
    with pytest.raises(ValueError, match="uncommitted changes"):
        benchmark.open_experiment(tmp_path, spec)

    # a different seed, or an environment that changed underneath (rebuilt image, other Python), is another experiment
    monkeypatch.setattr(benchmark, "_git_state", lambda: ("abc", False))
    for changed in ({**spec, "seed": SEED + 1}, {**spec, "environment": {**ENVIRONMENT, "python": "3.13.0"}}):
        with pytest.raises(ValueError, match="different experiment"):
            benchmark.open_experiment(tmp_path, changed)


# ----------------------------------------------------------------------------- ADR-010: budgets, spend, pacing


def _bench_args(**overrides) -> argparse.Namespace:
    base = dict(
        offline=False,
        cheap_model="p:cheap",
        strong_model="p:strong",
        planner_model="p:planner",
        worker_model="p:worker",
        max_tokens=1_000_000,
        max_attempt_tokens=250_000,
        max_cost_usd=20.0,
        max_wallclock_s=1800,
        max_replans=1,
        pace_s=0.0,
        cooldown_s=0.0,
        max_consecutive_infrastructure=3,
    )
    base.update(overrides)
    return argparse.Namespace(**base)


def test_c_and_d_execute_under_what_the_shared_plan_left_of_the_equal_total_budget():
    """ADR-010: A/B plan inside their run. If C/D also got the full budget after planning outside it, they would
    silently receive more than A/B — in the direction that flatters MAS."""
    args = _bench_args()
    plan = _plan(4, 1, call_input_tokens=30_000, call_output_tokens=10_000, call_cost_usd=1.25, plan_s=64.0)
    assert benchmark.execution_budgets(args, None) == benchmark.full_budgets(args)  # A and B
    assert benchmark.execution_budgets(args, plan) == {
        "max_tokens": 1_000_000 - 40_000,
        "max_cost_usd": 18.75,
        "max_wallclock_s": 1800 - 64,
    }
    argv = benchmark.command(args, "D", 4, Path("fixture.json"), Path("plan.json"), benchmark.execution_budgets(args, plan))
    assert argv[argv.index("--max-cost-usd") + 1] == "18.75"
    assert argv[argv.index("--max-tokens") + 1] == "960000"

    # unpriced planning subtracts nothing rather than guessing — that case stops the matrix through the ledger instead
    unpriced = _plan(4, 1, call_cost_usd=None, cost_known=False, call_input_tokens=10, call_output_tokens=0)
    assert benchmark.execution_budgets(args, unpriced)["max_cost_usd"] == 20.0


def test_planning_that_consumed_the_whole_budget_is_an_experimental_result_not_a_fresh_budget():
    args = _bench_args(max_cost_usd=1.0, max_tokens=1000, max_wallclock_s=60)
    plan = _plan(8, 2, call_input_tokens=900, call_output_tokens=200, call_cost_usd=1.0, plan_s=70.0)
    budgets = benchmark.execution_budgets(args, plan)
    assert budgets == {"max_tokens": 0, "max_cost_usd": 0.0, "max_wallclock_s": 0}
    row = benchmark.budget_exhausted_row("C", 8, 2, plan, budgets)
    assert classify_run(row) == "experimental" and row["status"] == "ABORTED"
    assert row["verdict_reason"] == "BUDGET_EXHAUSTED" and row["plan_sha256"] == _plan_sha(8, 2)


def test_system_totals_add_the_block_plan_back_so_c_d_can_be_compared_with_a_b():
    plan = _plan(4, 1, call_cost_usd=0.5, call_input_tokens=100, call_output_tokens=50, plan_s=12.0)
    d = benchmark.attach_system_totals(_row("D", 4, 1, call_cost_usd=2.0, machine_s=40.0), plan)
    assert (d["call_cost_usd"], d["machine_s"]) == (2.0, 40.0)  # execution-only: the C-vs-D contrast is unchanged
    assert (d["system_call_cost_usd"], d["system_machine_s"]) == (2.5, 52.0) and d["system_cost_known"]
    a = benchmark.attach_system_totals(_row("A", 4, 1, call_cost_usd=2.0, machine_s=40.0), None)
    assert (a["system_call_cost_usd"], a["system_machine_s"]) == (2.0, 40.0)  # A/B plan inside their run
    unpriced = benchmark.attach_system_totals(
        _row("C", 4, 1, call_cost_usd=2.0), _plan(4, 1, call_cost_usd=None, cost_known=False)
    )
    assert unpriced["system_call_cost_usd"] is None and not unpriced["system_cost_known"]


def test_spend_ledger_counts_every_billed_operation_including_superseded_reruns_and_retried_plans():
    """The cap must see what the provider billed, not what the summary keeps: an infrastructure row that was later
    rerun, and a planning round that had to be retried, were both paid for."""
    infra = _row("A", 1, 1, status="FAILED", verdict_reason="UNSUPPORTED", reason_text="verification not completed (INVALID)")
    rerun = _row("A", 1, 1, suffix=1)
    plans = [_plan(1, 1, planned=False, outcome="failed", call_cost_usd=0.5), _plan(1, 1, call_cost_usd=0.75)]
    ledger = benchmark.spend_ledger([infra, rerun], plans)
    assert ledger == {"spent_usd": 0.01 + 0.01 + 0.5 + 0.75, "unpriced_operations": 0, "operations": 4}

    # a run whose cost is unknown makes the total a floor, and is counted so the ceiling can refuse to continue
    lost = {"config": "B", "n": 1, "repetition": 2, "status": "CLIENT_ERROR"}
    assert benchmark.spend_ledger([lost], [])["unpriced_operations"] == 1
    # bookkeeping rows echo the planning round's cost for classification; counting them would bill the plan three times
    skipped = benchmark.plan_failure_row("C", 1, 1, plans[0])
    assert benchmark.spend_ledger([skipped], [])["spent_usd"] == 0.0


def test_no_operation_may_start_unless_the_ceiling_still_covers_its_own_maximum():
    owed = [("plan", 4, 1), ("C", 4, 1), ("D", 4, 1)]
    ledger = {"spent_usd": 40.0, "unpriced_operations": 0, "operations": 6}
    line, refusal = benchmark.spend_admission(ledger, 100.0, 20.0, owed, "C N=4 repeat=1")
    assert refusal is None and "billed $40.0000 of $100.0000" in line and "worst case <= $100.0000" in line

    _, refusal = benchmark.spend_admission({**ledger, "spent_usd": 85.0}, 100.0, 20.0, owed, "C N=4 repeat=1")
    assert refusal and "does not cover another $20.0000" in refusal  # stops *before* spending, not after

    _, refusal = benchmark.spend_admission({**ledger, "unpriced_operations": 1}, 100.0, 20.0, owed, "C")
    assert refusal and "unknown cost" in refusal
    _, allowed = benchmark.spend_admission({**ledger, "unpriced_operations": 1}, 100.0, 20.0, owed, "C", stop_on_unpriced=False)
    assert allowed is None  # ...unless the operator explicitly accepted unknown cost (--allow-unpriced)

    line, refusal = benchmark.spend_admission(ledger, None, 20.0, owed, "C")
    assert refusal is None and "no ceiling configured" in line


def test_worst_case_projection_counts_exactly_the_operations_still_owed():
    schedule = benchmark.build_schedule(["C", "D"], [4], 1, SEED)
    assert benchmark.pending_operations(schedule, {}, {}) == [("plan", 4, 1), ("C", 4, 1), ("D", 4, 1)]

    plans = {(4, 1): _plan(4, 1)}
    done = {("C", 4, 1): _row("C", 4, 1)}
    assert benchmark.pending_operations(schedule, done, plans) == [("D", 4, 1)]  # plan on record, C recorded

    invalid = _row("C", 4, 1, status="FAILED", verdict_reason="UNSUPPORTED", reason_text="verification not completed (INVALID)")
    owed = benchmark.pending_operations(schedule, {("C", 4, 1): invalid}, plans)
    assert owed == [("C", 4, 1), ("D", 4, 1)]  # an infrastructure-invalid cell is owed again, the usable plan is not


def test_consecutive_infrastructure_failures_stop_the_matrix_and_a_success_resets_the_breaker(monkeypatch):
    """A provider-wide incident must never be walked through automatically: every cell would be rerun anyway."""
    slept: list[float] = []
    monkeypatch.setattr(benchmark.time, "sleep", slept.append)
    args = _bench_args(pace_s=1.0, cooldown_s=30.0, max_consecutive_infrastructure=3)
    state = {"consecutive": 0}
    assert benchmark.after_operation(True, state, args) is None
    assert benchmark.after_operation(True, state, args) is None
    assert slept == [30.0, 30.0]  # a machinery failure waits longer than the pace
    assert benchmark.after_operation(False, state, args) is None and state["consecutive"] == 0 and slept[-1] == 1.0
    for _ in range(2):
        assert benchmark.after_operation(True, state, args) is None
    stop = benchmark.after_operation(True, state, args)
    assert stop and "3 consecutive infrastructure failures" in stop and "resume" in stop

    disabled = _bench_args(max_consecutive_infrastructure=0, cooldown_s=0.0)
    assert benchmark.after_operation(True, {"consecutive": 99}, disabled) is None


def test_a_live_matrix_refuses_to_start_without_a_ceiling_or_an_explicit_reasoning_effort(monkeypatch, tmp_path: Path):
    common = [
        "--cheap-model",
        "anthropic:cheap",
        "--strong-model",
        "anthropic:strong",
        "--planner-model",
        "anthropic:planner",
        "--worker-model",
        "anthropic:worker",
        "--repeats",
        "5",
        "--output",
        str(tmp_path),
        "--allow-dirty",
    ]
    monkeypatch.setenv("MAS_MODEL_PRICES", '{"m": [1.0, 2.0]}')
    monkeypatch.setenv("MAS_ANTHROPIC_EFFORT", "medium")
    with pytest.raises(SystemExit):  # no aggregate ceiling
        benchmark.main(common)

    monkeypatch.setenv("MAS_ANTHROPIC_EFFORT", "")
    with pytest.raises(SystemExit):  # ceiling given, but effort left to the provider's default
        benchmark.main([*common, "--max-total-cost-usd", "250"])


def test_the_spend_ceiling_is_recorded_with_an_append_only_history_rather_than_frozen_into_the_identity(
    tmp_path: Path, monkeypatch
):
    """Freezing the ceiling inside `spec` would make an experiment that reaches it unresumable — the operator's only
    way forward would be to discard evidence already paid for (ADR-010)."""
    spec = {"configs": CONFIGS, "widths": WIDTHS, "repeats": 5, "seed": SEED, "environment": ENVIRONMENT}
    monkeypatch.setattr(benchmark, "_git_state", lambda: ("abc", False))
    manifest = benchmark.open_experiment(tmp_path, spec, spend_cap_usd=250.0)
    assert manifest["spend_cap_usd"] == 250.0 and len(manifest["spend_cap_history"]) == 1

    same = benchmark.open_experiment(tmp_path, spec, spend_cap_usd=250.0)
    assert len(same["spend_cap_history"]) == 1  # unchanged: nothing to record
    raised = benchmark.open_experiment(tmp_path, spec, spend_cap_usd=400.0)
    assert raised["spend_cap_usd"] == 400.0 and len(raised["spend_cap_history"]) == 2
    assert raised["spend_cap_history"][-1]["from_usd"] == 250.0 and raised["spend_cap_history"][-1]["to_usd"] == 400.0
    on_disk = json.loads((tmp_path / "experiment.json").read_text(encoding="utf-8"))
    assert on_disk["spend_cap_usd"] == 400.0 and "max_total_cost_usd" not in json.dumps(on_disk["spec"])


def test_crossover_requires_parallel_to_preserve_success_and_improve_the_metric():
    summary = [
        {"config": "A", "n": 1, "success_rate": 1.0, "median_machine_s": 5.0},
        {"config": "D", "n": 1, "success_rate": 0.8, "median_machine_s": 2.0},
        {"config": "A", "n": 2, "success_rate": 1.0, "median_machine_s": 8.0},
        {"config": "D", "n": 2, "success_rate": 1.0, "median_machine_s": 4.0},
    ]
    assert crossover(summary, "A", "median_machine_s") == 2


# ----------------------------------------------------------------------------- live smoke resumption


_LIVE_SETUP = {
    "workers": 3,
    "max_concurrency": 3,
    "lease_s": 15,
    "max_wallclock_s": 1800,
    "max_attempt_runtime_s": 600,
    "max_tokens": 1_500_000,
    "max_attempt_tokens": 250_000,
    "max_cost_usd": 10.0,
    "max_replans": 1,
    "max_attempts_per_task": 2,
}


_LIVE_SHAPE = {
    "anthropic_thinking": True,
    "anthropic_effort": "medium",
    "anthropic_fallbacks": "",
    "provider_timeout_s": 600.0,
    "provider_max_retries": 2,
    "attempt_max_calls": 40,
}


def _live_evidence(**overrides) -> dict:
    base = {
        "schema": 1,
        "started_at": "t0",
        "git": {"commit": COMMIT, "dirty": False},
        "models": {"worker": "p:w", "planner": "p:s"},
        "requested_steps": ["ping", "worker", "planner", "repair"],
        "manual_contract_approval": True,
        "allow_unpriced": False,
        "model_prices": {"m": [1.0, 2.0]},
        "setup": dict(_LIVE_SETUP),
        "request_shape": dict(_LIVE_SHAPE),
        "steps": {},
        "runs": [],
        "complete": False,
    }
    base.update(overrides)
    return base


def test_live_smoke_resume_carries_passed_stages_only_from_identical_setup():
    previous = _live_evidence(
        steps={"ping": True, "worker": True, "planner": False},
        runs=[{"step": "worker", "status": "PASSED", "priced": True}, {"step": "planner", "status": "FAILED", "priced": True}],
    )
    merged, skip, refusal = live_smoke.merge_resume(previous, _live_evidence(started_at="t1"))
    assert refusal is None and skip == ["ping", "worker"]
    assert merged["steps"] == {"ping": True, "worker": True} and [r["step"] for r in merged["runs"]] == ["worker"]
    assert merged["resumed_from"] == {"started_at": "t0", "steps": ["ping", "worker"]}

    for change in (
        {"git": {"commit": "other", "dirty": False}},
        {"git": {"commit": COMMIT, "dirty": True}},
        {"models": {"worker": "p:x", "planner": "p:s"}},
        {"manual_contract_approval": False},
        {"allow_unpriced": True},
        {"schema": 2},
        # a different price table, budget, worker count, concurrency or replan limit is a different experiment
        {"model_prices": {"m": [1.0, 3.0]}},
        {"model_prices": {}},
        {"setup": {**_LIVE_SETUP, "max_tokens": 2_000_000}},
        {"setup": {**_LIVE_SETUP, "max_cost_usd": 20.0}},
        {"setup": {**_LIVE_SETUP, "max_wallclock_s": 900}},
        {"setup": {**_LIVE_SETUP, "workers": 1}},
        {"setup": {**_LIVE_SETUP, "max_concurrency": 1}},
        {"setup": {**_LIVE_SETUP, "max_replans": 2}},
        {"setup": None},
        # a stage that passed at another reasoning effort (or timeout/retry shape) cost something else and may well
        # have behaved differently — it is not evidence for this one (ADR-010)
        {"request_shape": {**_LIVE_SHAPE, "anthropic_effort": "high"}},
        {"request_shape": {**_LIVE_SHAPE, "anthropic_thinking": False}},
        {"request_shape": None},
    ):
        current = _live_evidence(started_at="t1")
        _, skip, refusal = live_smoke.merge_resume(_live_evidence(steps={"ping": True}, **change), current)
        assert refusal and skip == [], change


def test_live_smoke_evidence_records_the_full_experimental_setup(monkeypatch):
    """The identity fields the resume compares are the ones the script actually records (prices + every budget)."""
    monkeypatch.setenv("MAS_MODEL_PRICES", '{"m": [1.0, 2.0]}')
    args = argparse.Namespace(
        workers=3,
        max_concurrency=3,
        max_wallclock_s=1800,
        max_attempt_runtime_s=600,
        max_tokens=1_500_000,
        max_attempt_tokens=250_000,
        max_cost_usd=10.0,
        max_replans=1,
    )
    from dataclasses import asdict

    setup = live_smoke._setup(args)
    assert setup == {"workers": 3, **asdict(live_smoke._budgets(args))}  # every Budgets field, not a hand-picked few
    assert {"workers", "max_concurrency", "max_tokens", "max_attempt_tokens", "max_cost_usd", "max_wallclock_s"} <= set(setup)
    assert {"max_attempt_runtime_s", "max_replans", "max_attempts_per_task", "lease_s"} <= set(setup)
    assert live_smoke._price_snapshot() == {"m": [1.0, 2.0]}
    identity = {"git", "models", "manual_contract_approval", "allow_unpriced", "model_prices", "setup", "request_shape"}
    assert set(live_smoke._RESUME_IDENTITY) >= identity


def test_live_smoke_request_shape_records_what_the_matrix_will_freeze(monkeypatch):
    monkeypatch.setenv("MAS_ANTHROPIC_EFFORT", "medium")
    monkeypatch.setenv("MAS_ANTHROPIC_THINKING", "1")
    shape = live_smoke._request_shape()
    assert shape["anthropic_effort"] == "medium" and shape["anthropic_thinking"] is True
    assert {"provider_timeout_s", "provider_max_retries", "attempt_max_calls"} <= set(shape)


def _ping_record(*, priced: bool = True, ok: bool = True, cost: float = 0.0004) -> dict:
    """What `cli.ping_spec` returns: the telemetry of the one metered call, as data."""
    return {
        "role": "worker",
        "spec": "p:w",
        "ok": ok,
        "error": None if ok else "ProviderRequestError: 401",
        "stop_reason": "end_turn" if ok else None,
        "text": "OK" if ok else "",
        "calls": [{"model": "m-1", "priced": priced, "cost_usd": cost if priced else 0.0}] if ok else [],
        "models": ["m-1"] if ok else [],
        "input_tokens": 15,
        "output_tokens": 2,
        "cache_read_tokens": 0,
        "duration_ms": 900,
        "priced": priced,
        "cost_usd": cost if priced else None,
    }


def _ping_args(**overrides) -> argparse.Namespace:
    base = dict(
        worker="p:w",
        ping_max_tokens=64,
        allow_unpriced=False,
        max_cost_usd=10.0,
        max_total_cost_usd=30.0,
        evidence={"runs": []},
        output=None,
    )
    base.update(overrides)
    return argparse.Namespace(**base)


def test_the_ping_is_billed_to_the_smoke_ledger_and_an_unpriced_one_fails_the_gate(monkeypatch):
    """It is one small call, but "small" is not "unaccounted": an unpriced ping means the price table does not cover
    the model the provider reported, and every cost figure after it would be a floor (ADR-010 / D15)."""
    args = _ping_args()
    monkeypatch.setattr(live_smoke.cli, "ping_spec", lambda *a, **kw: _ping_record())
    assert live_smoke.step_ping(args) is True
    assert args.evidence["ping"]["models"] == ["m-1"]
    spend = live_smoke._spend(args)
    assert spend["billed_usd"] == 0.0004 and spend["by_step"]["ping"] == 0.0004 and spend["unpriced_calls"] == 0

    unpriced = _ping_args()
    monkeypatch.setattr(live_smoke.cli, "ping_spec", lambda *a, **kw: _ping_record(priced=False))
    assert live_smoke.step_ping(unpriced) is False  # completion fails: the cost of everything after is unknowable
    spend = live_smoke._spend(unpriced)
    assert spend["billed_usd"] == 0.0 and spend["unpriced_calls"] == 1 and spend["by_step"]["ping"] is None
    assert live_smoke.step_ping(_ping_args(allow_unpriced=True)) is True  # ...unless explicitly accepted

    failed = _ping_args()
    monkeypatch.setattr(live_smoke.cli, "ping_spec", lambda *a, **kw: _ping_record(ok=False))
    assert live_smoke.step_ping(failed) is False
    assert live_smoke._spend(failed)["billed_usd"] == 0.0  # a call that never happened bills nothing


def test_a_paid_ping_is_carried_forward_exactly_once_and_never_billed_twice():
    previous = _live_evidence(steps={"ping": True}, ping=_ping_record())
    merged, skip, refusal = live_smoke.merge_resume(previous, _live_evidence(started_at="t1"))
    assert refusal is None and skip == ["ping"] and merged["ping"] == previous["ping"]
    args = argparse.Namespace(evidence=merged, max_total_cost_usd=30.0, max_cost_usd=10.0)
    assert live_smoke._spend(args)["billed_usd"] == 0.0004  # counted once, from the carried record

    # a ping that is going to be re-run must not carry the earlier charge into the new evidence
    stale = _live_evidence(steps={"worker": True}, ping=_ping_record())
    merged, skip, _ = live_smoke.merge_resume(stale, _live_evidence(started_at="t1"))
    assert skip == ["worker"] and "ping" not in merged


def test_an_exhausted_smoke_ceiling_stops_even_the_ping():
    args = _ping_args(max_total_cost_usd=1.0, evidence={"runs": [{"step": "worker", "metrics": {"call_cost_usd": 1.0}}]})
    assert live_smoke._admit(args, "ping").startswith("the $1.0000 smoke ceiling is already spent")
    args.max_total_cost_usd = 1.5  # any headroom admits it; its cost is billed afterwards like everything else
    assert live_smoke._admit(args, "ping") is None


def test_mvp_gate_rejects_evidence_whose_ping_was_unpriced(tmp_path: Path):
    live, distributed, bench = tmp_path / "live.json", tmp_path / "distributed.json", tmp_path / "bench"
    _write_live_and_distributed(live, distributed)
    _write_matrix(bench, _rows())
    evidence = json.loads(live.read_text(encoding="utf-8"))
    evidence["ping"] = _ping_record(priced=False)
    _write(live, evidence)
    assert "live.priced" in _failed(evaluate(live, distributed, bench, current_git=(COMMIT, False)))

    evidence["ping"] = _ping_record()
    _write(live, evidence)
    assert not _failed(evaluate(live, distributed, bench, current_git=(COMMIT, False)))


def test_live_smoke_is_bounded_as_a_whole_and_reports_what_each_stage_cost(capsys):
    """The smoke is the first thing that spends money and the run that tells you what a run costs (ADR-010)."""
    args = argparse.Namespace(max_cost_usd=10.0, max_total_cost_usd=30.0, evidence={"runs": []})
    assert live_smoke._spend(args) == {"billed_usd": 0.0, "unpriced_calls": 0, "ceiling_usd": 30.0, "by_step": {}}
    assert live_smoke._admit(args, "worker") is None

    args.evidence["runs"] = [
        {"step": "worker", "metrics": {"call_cost_usd": 4.25, "unpriced_calls": 0}},
        {"step": "planner", "metrics": {"call_cost_usd": 6.5, "unpriced_calls": 0}},
    ]
    spend = live_smoke._spend(args)
    assert spend["billed_usd"] == 10.75 and spend["by_step"] == {"worker": 4.25, "planner": 6.5}
    assert live_smoke._admit(args, "repair") is None

    args.max_total_cost_usd = 15.0  # 10.75 billed + another 10.00 run would cross it
    stop = live_smoke._admit(args, "repair")
    assert stop and "does not cover another $10.0000" in stop
    assert live_smoke._admit(args, "ping") is None  # one metered call is never worth refusing

    # an unpriced stage makes the total a floor: reported, never folded in as zero
    args.evidence["runs"].append({"step": "repair", "metrics": {"call_cost_usd": None, "unpriced_calls": 3}})
    spend = live_smoke._spend(args)
    assert spend["billed_usd"] == 10.75 and spend["unpriced_calls"] == 3 and spend["by_step"]["repair"] is None
    capsys.readouterr()
    live_smoke._admit(args, "ping")
    assert "unpriced call(s) so far" in capsys.readouterr().out  # unknown spend is stated, not hidden


def test_distributed_smoke_refuses_running_actors_but_reuses_the_shared_postgres():
    """The documented setup keeps `postgres` up before `mas doctor`/`mas up`; only another stack's actors block."""
    assert distributed_smoke.blocking_services(set()) == set()
    assert distributed_smoke.blocking_services({"postgres"}) == set()
    assert distributed_smoke.blocking_services({"postgres", "worker"}) == {"worker"}
    assert distributed_smoke.blocking_services({"orchestrator", "gateway", "worker", "postgres"}) == {
        "orchestrator",
        "gateway",
        "worker",
    }


@pytest.mark.parametrize(
    "script",
    ["live_smoke.py", "benchmark.py", "distributed_smoke.py", "mvp_gate.py", "test.py"],
)
def test_release_script_help_is_windows_console_safe(script: str):
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "cp1252"
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / script), "--help"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr.decode("cp1252", errors="replace")
