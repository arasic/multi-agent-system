"""Run the frozen M3 A/B/C/D x N matrix and write machine-readable evidence plus a compact SVG report.

Real evaluation (minimum five repetitions per cell):
  python scripts/benchmark.py --cheap-model openai:... --strong-model anthropic:... \
      --planner-model anthropic:... --worker-model openai:... --repeats 5

Key-less substrate rehearsal (hand-written width DAG, stub agents, real verifier):
  python scripts/benchmark.py --offline --repeats 1

Every cell uses the same goal, acceptance suite and total budgets. A/B are produced by the runtime's single-agent
policy; C/D use the same planner/worker models and differ only in concurrency. JSONL is appended after each run so an
interrupted experiment remains auditable. Unpriced calls make cost null in the summary rather than falsely cheap.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import statistics
import subprocess
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from mas.config import settings  # noqa: E402
from mas.db import connect  # noqa: E402
from mas.evaluation import CONFIGS, WIDTHS, width_dag, width_goal  # noqa: E402
from mas.metrics import compute  # noqa: E402

RUN_ID = re.compile(r"\brun ([0-9a-f-]{36})\b", re.I)
TERMINAL = {"PASSED", "FAILED", "ABORTED"}


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


def experiment_spec(args) -> dict:
    """Everything that must remain frozen across a resumable experiment."""
    prices = settings().model_prices.strip()
    try:
        price_snapshot = json.loads(prices) if prices else {}
    except json.JSONDecodeError:
        price_snapshot = prices
    return {
        "schema": 1,
        "mode": "offline" if args.offline else "live",
        "configs": list(args.configs),
        "widths": list(args.widths),
        "repeats": args.repeats,
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
    }


def open_experiment(output: Path, spec: dict) -> dict:
    """Create or resume one immutable experiment. Refuse to mix configurations in the same evidence directory."""
    path = output / "experiment.json"
    revision, dirty = _git_state()
    if path.exists():
        manifest = json.loads(path.read_text(encoding="utf-8"))
        if manifest.get("spec") != spec:
            raise ValueError(f"{path} belongs to a different experiment; choose a new --output directory")
        if manifest.get("git_commit") != revision:
            raise ValueError(f"{path} was started at commit {manifest.get('git_commit')}; current commit is {revision}")
        if manifest.get("git_dirty") is False and dirty:
            raise ValueError("the experiment started from a clean tree but the current tree has uncommitted changes")
        return manifest
    manifest = {
        "schema": 1,
        "experiment_id": str(uuid4()),
        "created_at": datetime.now(UTC).isoformat(),
        "git_commit": revision,
        "git_dirty": dirty,
        "spec": spec,
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


def completion(rows: list[dict], *, configs: list[str], widths: list[int], repeats: int, require_priced: bool) -> dict:
    expected_keys = {(c, n, r) for c in configs for n in widths for r in range(1, repeats + 1)}
    actual_keys = {(r.get("config"), r.get("n"), r.get("repetition")) for r in rows}
    duplicates = len(rows) - len(actual_keys)
    missing = sorted(expected_keys - actual_keys)
    foreign = sorted(actual_keys - expected_keys, key=str)
    invalid = [r for r in rows if r.get("status") not in TERMINAL]
    unpriced = [r for r in rows if not r.get("cost_known", False)] if require_priced else []
    evidence_complete = not missing and not foreign and duplicates == 0 and not invalid and not unpriced
    return {
        "expected_runs": len(expected_keys),
        "recorded_runs": len(rows),
        "evidence_complete": evidence_complete,
        "all_passed": evidence_complete and all(r.get("status") == "PASSED" for r in rows),
        "missing": [{"config": c, "n": n, "repetition": r} for c, n, r in missing],
        "foreign_rows": len(foreign),
        "duplicate_rows": duplicates,
        "invalid_rows": len(invalid),
        "unpriced_rows": len(unpriced),
    }


def command(args, config: str, n: int, dag_file: Path) -> list[str]:
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
        str(args.max_tokens),
        "--max-attempt-tokens",
        str(args.max_attempt_tokens),
        "--max-cost-usd",
        str(args.max_cost_usd),
        "--max-wallclock-s",
        str(args.max_wallclock_s),
        "--max-replans",
        str(args.max_replans),
    ]
    if args.offline:
        return common + [
            "--dag",
            str(dag_file),
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
    concurrency = 1 if config == "C" else n
    return common + [
        "--goal",
        width_goal(n),
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
        return row
    run_id = UUID(match.group(1))
    with connect() as conn:
        m = compute(conn, run_id)
    row.update(m.as_dict())
    row["cost_known"] = m.unpriced_calls == 0
    if m.unpriced_calls:
        row["cost_usd"] = None
        row["call_cost_usd"] = None
    return row


def aggregate(rows: list[dict]) -> list[dict]:
    out = []
    for config in CONFIGS:
        for n in WIDTHS:
            cell = [r for r in rows if r["config"] == config and r["n"] == n]
            if not cell:
                continue
            passed = [r for r in cell if r.get("status") == "PASSED"]
            times = [float(r["machine_s"]) for r in passed if r.get("machine_s") is not None]
            costs = [float(r["call_cost_usd"]) for r in passed if r.get("call_cost_usd") is not None]
            out.append(
                {
                    "config": config,
                    "n": n,
                    "runs": len(cell),
                    "passes": len(passed),
                    "success_rate": round(len(passed) / len(cell), 4),
                    "median_machine_s": round(statistics.median(times), 3) if times else None,
                    "median_cost_usd": round(statistics.median(costs), 6) if len(costs) == len(passed) and costs else None,
                }
            )
    return out


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


def write_analysis(summary: list[dict], manifest: dict, done: dict, path: Path) -> None:
    by = {(r["config"], r["n"]): r for r in summary}
    lines = [
        "# Frozen M3 result",
        "",
        f"Experiment: `{manifest['experiment_id']}`  ",
        f"Git commit: `{manifest['git_commit']}`  ",
        f"Evidence complete: **{done['evidence_complete']}** ({done['recorded_runs']}/{done['expected_runs']} runs)  ",
        f"All runs passed: **{done['all_passed']}**",
        "",
        "Failures are experimental outcomes, not discarded retries. Medians use passing runs; success rate uses every run.",
        "",
        "| N | Config | Runs | Success | Median machine s | Median cost USD |",
        "|---:|:------:|-----:|--------:|-----------------:|----------------:|",
    ]
    for n in manifest["spec"]["widths"]:
        for config in manifest["spec"]["configs"]:
            row = by.get((config, n), {})
            machine = "—" if row.get("median_machine_s") is None else str(row["median_machine_s"])
            cost = "—" if row.get("median_cost_usd") is None else str(row["median_cost_usd"])
            success = "—" if row.get("success_rate") is None else f"{100 * row['success_rate']:.1f}%"
            lines.append(f"| {n} | {config} | {row.get('runs', 0)} | {success} | {machine} | {cost} |")
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
        "This report is descriptive. A negative result is a completed MVP result; do not tune or omit cells after "
        "seeing outcomes.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


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
    args = ap.parse_args(argv)
    if args.repeats < 1:
        ap.error("--repeats must be positive")
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
    if args.dry_run:
        manifest = {"experiment_id": "dry-run"}
        rows: list[dict] = []
    else:
        args.output.mkdir(parents=True, exist_ok=True)
        try:
            manifest = open_experiment(args.output, experiment_spec(args))
            rows = load_rows(args.output / "runs.jsonl", manifest["experiment_id"])
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            ap.error(str(exc))
    jsonl = args.output / "runs.jsonl"
    completed = {(r["config"], r["n"], r["repetition"]) for r in rows}
    with tempfile.TemporaryDirectory(prefix="mas-benchmark-") as tmp:
        tmp = Path(tmp)
        for n in args.widths:
            dag_file = tmp / f"adapters_{n}.json"
            dag_file.write_text(json.dumps(width_dag(n).to_dict(), indent=2), encoding="utf-8")
            for config in args.configs:
                argv2 = command(args, config, n, dag_file)
                for repetition in range(1, args.repeats + 1):
                    if args.dry_run:
                        print(" ".join(argv2))
                        continue
                    key = (config, n, repetition)
                    if key in completed:
                        print(f"[resume] {config} N={n} repeat={repetition} already recorded", flush=True)
                        continue
                    row = run_one(argv2, config=config, n=n, repetition=repetition)
                    row["experiment_id"] = manifest["experiment_id"]
                    row["git_commit"] = manifest["git_commit"]
                    row["suite_sha256"] = manifest["spec"]["suite_sha256"][str(n)]
                    rows.append(row)
                    with jsonl.open("a", encoding="utf-8") as f:
                        f.write(json.dumps(row, default=str) + "\n")
    if args.dry_run:
        return 0
    summary = aggregate(rows)
    with (args.output / "summary.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(summary[0]) if summary else ["config", "n"])
        writer.writeheader()
        writer.writerows(summary)
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
    write_analysis(summary, manifest, done, args.output / "analysis.md")
    print(
        f"\nresults: {args.output} ({len(rows)}/{done['expected_runs']} runs; "
        f"evidence_complete={done['evidence_complete']}; all_passed={done['all_passed']})"
    )
    # A model/task failure is valid experimental data. Missing/duplicate/client-error/unpriced evidence is not.
    return 0 if done["evidence_complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
