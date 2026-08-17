"""The MVP evidence gate and the M3 runner's evidence integrity: raw rows are the truth, summaries must agree, evidence
binds to one clean commit, infrastructure failures are rerun rather than counted, and the report is regenerable."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import benchmark, live_smoke
from scripts.benchmark import classify_run, completion, crossover, effective_rows
from scripts.mvp_gate import evaluate

ROOT = Path(__file__).resolve().parents[1]
CONFIGS = list("ABCD")
WIDTHS = [1, 2, 4, 8, 16]
COMMIT = "abc"
SUITES = {str(n): f"suite{n}" for n in WIDTHS}
EXPERIMENT = "exp-1"


def _row(config: str, n: int, repetition: int, *, status: str = "PASSED", priced: bool = True, **extra) -> dict:
    """A row shaped like `benchmark.run_one` + the manifest stamps."""
    row = {
        "config": config,
        "n": n,
        "repetition": repetition,
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


def _manifest() -> dict:
    return {
        "schema": 1,
        "experiment_id": EXPERIMENT,
        "git_commit": COMMIT,
        "git_dirty": False,
        "spec": {
            "mode": "live",
            "configs": CONFIGS,
            "widths": WIDTHS,
            "repeats": 5,
            "models": {"cheap": "a", "strong": "b", "planner": "c", "worker": "d"},
            "budgets": {"max_tokens": 1, "max_attempt_tokens": 1, "max_cost_usd": 1.0, "max_wallclock_s": 1, "max_replans": 1},
            "model_prices": {"a": {"input": 1, "output": 2}},
            "suite_sha256": SUITES,
        },
    }


def _write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _write_matrix(bench: Path, rows: list[dict], *, manifest: dict | None = None) -> dict:
    """Exactly what `benchmark.py` leaves behind: manifest, raw rows, recomputed completion, rendered report."""
    manifest = manifest or _manifest()
    _write(bench / "experiment.json", manifest)
    bench.mkdir(parents=True, exist_ok=True)
    (bench / "runs.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    spec = manifest["spec"]
    done = completion(rows, configs=spec["configs"], widths=spec["widths"], repeats=spec["repeats"], require_priced=True)
    done.update({"schema": 1, "experiment_id": EXPERIMENT, "updated_at": "t"})
    _write(bench / "completion.json", done)
    benchmark.write_analysis(benchmark.aggregate(rows), manifest, done, bench / "analysis.md")
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
        (
            _failed_row("FAIL:verification failed; repair needs a planner and none is configured (--planner)", UNRECOVERABLE),
            "infrastructure",
        ),
        # a task exhausted its attempts: infrastructure only when *every* failed attempt was the machinery's
        (_failed_row(RETRIES, UNRECOVERABLE, attempt_failure_classes={"infrastructure": 3}), "infrastructure"),
        (_failed_row(RETRIES, UNRECOVERABLE, attempt_failure_classes={"infrastructure": 2, "model": 1}), "experimental"),
        (_failed_row(RETRIES, UNRECOVERABLE, attempt_failure_classes={"model": 3, "abandoned": 1}), "experimental"),
        (_failed_row(RETRIES, UNRECOVERABLE, attempt_failure_classes={"abandoned": 3}), "experimental"),
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
    invalid = _row("D", 4, 5, status="FAILED", verdict_reason="UNSUPPORTED", reason_text="not completed (INVALID)", suffix=1)
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


def test_mvp_evidence_reports_missing_files(tmp_path: Path):
    with pytest.raises(ValueError, match="missing evidence"):
        evaluate(tmp_path / "missing.json", tmp_path / "distributed.json", tmp_path / "bench", current_git=(COMMIT, False))


def test_resumed_experiment_refuses_a_changed_revision_or_dirty_tree(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(benchmark, "_git_state", lambda: ("abc", False))
    manifest = benchmark.open_experiment(tmp_path, {"frozen": True})
    assert manifest["git_commit"] == "abc" and not manifest["git_dirty"]

    monkeypatch.setattr(benchmark, "_git_state", lambda: ("def", False))
    with pytest.raises(ValueError, match="current commit is def"):
        benchmark.open_experiment(tmp_path, {"frozen": True})

    monkeypatch.setattr(benchmark, "_git_state", lambda: ("abc", True))
    with pytest.raises(ValueError, match="uncommitted changes"):
        benchmark.open_experiment(tmp_path, {"frozen": True})


def test_crossover_requires_parallel_to_preserve_success_and_improve_the_metric():
    summary = [
        {"config": "A", "n": 1, "success_rate": 1.0, "median_machine_s": 5.0},
        {"config": "D", "n": 1, "success_rate": 0.8, "median_machine_s": 2.0},
        {"config": "A", "n": 2, "success_rate": 1.0, "median_machine_s": 8.0},
        {"config": "D", "n": 2, "success_rate": 1.0, "median_machine_s": 4.0},
    ]
    assert crossover(summary, "A", "median_machine_s") == 2


# ----------------------------------------------------------------------------- live smoke resumption


def _live_evidence(**overrides) -> dict:
    base = {
        "schema": 1,
        "started_at": "t0",
        "git": {"commit": COMMIT, "dirty": False},
        "models": {"worker": "p:w", "planner": "p:s"},
        "requested_steps": ["ping", "worker", "planner", "repair"],
        "manual_contract_approval": True,
        "allow_unpriced": False,
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
    ):
        current = _live_evidence(started_at="t1")
        _, skip, refusal = live_smoke.merge_resume(_live_evidence(steps={"ping": True}, **change), current)
        assert refusal and skip == [], change


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
