"""Audit whether the frozen MVP has complete, reproducible live evidence.

This does not call a provider or spend money. The two evidence producers are:

  python scripts/live_smoke.py --worker ... --planner ... --step all --no-auto-approve \
      --output mvp-evidence/live-smoke.json
  python scripts/distributed_smoke.py --build --output mvp-evidence/distributed-smoke.json
  python scripts/benchmark.py --cheap-model ... --strong-model ... --planner-model ... --worker-model ... \
      --repeats 5 --output benchmark-results

Then:

  python scripts/mvp_gate.py

Exit 0 means the direct live-model gate, distributed operator gate, and frozen M3 100-run matrix are complete on the
same clean commit, with priced calls and a manually approved acceptance contract. It does not require MAS to win.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
CONFIGS = {"A", "B", "C", "D"}
WIDTHS = {1, 2, 4, 8, 16}


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


def evaluate(live_path: Path, distributed_path: Path, benchmark_dir: Path) -> list[Check]:
    live = _read(live_path)
    distributed = _read(distributed_path)
    manifest = _read(benchmark_dir / "experiment.json")
    done = _read(benchmark_dir / "completion.json")
    spec = manifest.get("spec") if isinstance(manifest.get("spec"), dict) else {}
    live_git = live.get("git") if isinstance(live.get("git"), dict) else {}
    distributed_git = distributed.get("git") if isinstance(distributed.get("git"), dict) else {}
    distributed_run = distributed.get("run") if isinstance(distributed.get("run"), dict) else {}
    runs = live.get("runs") if isinstance(live.get("runs"), list) else []
    steps = live.get("steps") if isinstance(live.get("steps"), dict) else {}
    model_specs = spec.get("models") if isinstance(spec.get("models"), dict) else {}
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
            bool(runs) and all(isinstance(r, dict) and r.get("priced") is True for r in runs),
            "every live run must have known model prices",
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
        Check("matrix.live", spec.get("mode") == "live", f"mode={spec.get('mode')}"),
        Check("matrix.configs", set(spec.get("configs") or []) == CONFIGS, f"configs={spec.get('configs')}"),
        Check("matrix.widths", set(spec.get("widths") or []) == WIDTHS, f"widths={spec.get('widths')}"),
        Check("matrix.repetitions", int(spec.get("repeats") or 0) >= 5, f"repeats={spec.get('repeats')}"),
        Check(
            "matrix.models",
            all(model_specs.get(k) for k in ("cheap", "strong", "planner", "worker")),
            f"models={model_specs}",
        ),
        Check("matrix.prices", bool(spec.get("model_prices")), "a frozen price snapshot is required"),
        Check("matrix.clean_git", manifest.get("git_dirty") is False, f"commit={manifest.get('git_commit')}"),
        Check(
            "matrix.complete",
            done.get("evidence_complete") is True and int(done.get("expected_runs") or 0) >= 100,
            f"recorded={done.get('recorded_runs')}/{done.get('expected_runs')}",
        ),
        Check("matrix.priced", int(done.get("unpriced_rows") or 0) == 0, f"unpriced={done.get('unpriced_rows')}"),
        Check(
            "matrix.analysis",
            (benchmark_dir / "analysis.md").is_file(),
            "generated crossover report",
        ),
        Check(
            "same_revision",
            bool(live_git.get("commit"))
            and live_git.get("commit") == distributed_git.get("commit") == manifest.get("git_commit"),
            f"live={live_git.get('commit')} distributed={distributed_git.get('commit')} "
            f"matrix={manifest.get('git_commit')}",
        ),
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
            print(f"{'PASS' if c.ok else 'FAIL':4s} {c.name:32s} {c.detail}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
