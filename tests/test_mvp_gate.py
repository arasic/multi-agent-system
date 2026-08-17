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
    }
    row.update(extra)
    return row


def _rows(*, priced: bool = True, status: str = "PASSED") -> list[dict]:
    return [_row(c, n, r, status=status, priced=priced) for c in CONFIGS for n in WIDTHS for r in range(1, 6)]


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
SEED = 7


def _manifest() -> dict:
    return {
        "schema": 2,
        "experiment_id": EXPERIMENT,
        "git_commit": COMMIT,
        "git_dirty": False,
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
        "schedule": benchmark.build_schedule(CONFIGS, WIDTHS, 5, SEED),
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

    rows = _rows()
    rows[3] = _row("A", 1, 4, status="FAILED", verdict_reason="UNSUPPORTED", reason_text="verification not completed (INVALID)")
    _write_matrix(bench, rows)
    failed = _failed(evaluate(live, distributed, bench, current_git=(COMMIT, False)))
    assert failed == {"matrix.complete", "matrix.no_infrastructure_invalid"}
    # ...and passes again once the cell has been rerun (append-only: the invalid row stays on record)
    _write_matrix(bench, rows + [_row("A", 1, 4, suffix=1)])
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
    rows = [
        benchmark.plan_failure_row(r["config"], 8, 3, {"status": "FAILED", "verdict": "no plan", **dead})
        | {"experiment_id": EXPERIMENT, "git_commit": COMMIT, "suite_sha256": SUITES["8"]}
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
    identity = {"git", "models", "manual_contract_approval", "allow_unpriced", "model_prices", "setup"}
    assert set(live_smoke._RESUME_IDENTITY) >= identity


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
