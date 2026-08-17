import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import benchmark
from scripts.benchmark import completion, crossover
from scripts.mvp_gate import evaluate

ROOT = Path(__file__).resolve().parents[1]


def _rows(*, priced: bool = True, status: str = "PASSED") -> list[dict]:
    return [
        {"config": c, "n": n, "repetition": r, "status": status, "cost_known": priced}
        for c in ("A", "B", "C", "D")
        for n in (1, 2, 4, 8, 16)
        for r in range(1, 6)
    ]


def test_matrix_completion_accepts_terminal_failures_as_data_but_not_missing_or_unpriced_rows():
    rows = _rows(status="FAILED")
    done = completion(rows, configs=list("ABCD"), widths=[1, 2, 4, 8, 16], repeats=5, require_priced=True)
    assert done["evidence_complete"] and not done["all_passed"] and done["expected_runs"] == 100

    assert not completion(rows[:-1], configs=list("ABCD"), widths=[1, 2, 4, 8, 16], repeats=5, require_priced=True)[
        "evidence_complete"
    ]
    assert not completion(_rows(priced=False), configs=list("ABCD"), widths=[1, 2, 4, 8, 16], repeats=5, require_priced=True)[
        "evidence_complete"
    ]
    assert not completion(rows + [rows[0]], configs=list("ABCD"), widths=[1, 2, 4, 8, 16], repeats=5, require_priced=True)[
        "evidence_complete"
    ]


def _write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def test_mvp_evidence_requires_all_live_stages_and_the_full_matrix_on_one_clean_revision(tmp_path: Path):
    live = tmp_path / "live.json"
    distributed = tmp_path / "distributed.json"
    bench = tmp_path / "bench"
    _write(
        live,
        {
            "complete": True,
            "steps": {"ping": True, "worker": True, "planner": True, "repair": True},
            "runs": [
                {"status": "PASSED", "priced": True},
                {"status": "PASSED", "priced": True},
                {"status": "PASSED", "priced": True},
            ],
            "manual_contract_approval": True,
            "git": {"commit": "abc", "dirty": False},
        },
    )
    _write(
        distributed,
        {
            "complete": True,
            "mode": "live",
            "priced": True,
            "run": {"status": "PASSED"},
            "git": {"commit": "abc", "dirty": False},
        },
    )
    _write(
        bench / "experiment.json",
        {
            "git_commit": "abc",
            "git_dirty": False,
            "spec": {
                "mode": "live",
                "configs": list("ABCD"),
                "widths": [1, 2, 4, 8, 16],
                "repeats": 5,
                "models": {"cheap": "a", "strong": "b", "planner": "c", "worker": "d"},
                "model_prices": {"a": {"input": 1, "output": 2}},
            },
        },
    )
    _write(
        bench / "completion.json",
        {"evidence_complete": True, "expected_runs": 100, "recorded_runs": 100, "unpriced_rows": 0},
    )
    (bench / "analysis.md").write_text("# result\n", encoding="utf-8")
    checks = evaluate(live, distributed, bench)
    assert checks and all(c.ok for c in checks)

    broken = json.loads(live.read_text(encoding="utf-8"))
    broken["steps"]["repair"] = False
    _write(live, broken)
    assert not all(c.ok for c in evaluate(live, distributed, bench))


def test_mvp_evidence_reports_missing_files(tmp_path: Path):
    with pytest.raises(ValueError, match="missing evidence"):
        evaluate(tmp_path / "missing.json", tmp_path / "distributed.json", tmp_path / "bench")


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
