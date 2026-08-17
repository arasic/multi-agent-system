"""Run the frozen M3 A/B/C/D × N matrix and write machine-readable evidence plus a compact SVG report.

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
import json
import re
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path
from uuid import UUID

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from mas.db import connect  # noqa: E402
from mas.evaluation import CONFIGS, WIDTHS, width_dag, width_goal  # noqa: E402
from mas.metrics import compute  # noqa: E402

RUN_ID = re.compile(r"\brun ([0-9a-f-]{36})\b", re.I)


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
            f'<path d="M{x0},{y0} V{y0+ph} H{x0+pw}" fill="none" stroke="#777"/>',
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
                chunks.append(
                    f'<polyline points="{points}" fill="none" stroke="{colors[config]}" stroke-width="2"/>'
                )
        for i, n in enumerate(WIDTHS):
            x = x0 + i * pw / (len(WIDTHS) - 1)
            chunks.append(
                f'<text x="{x:.1f}" y="{y0+ph+18}" text-anchor="middle" '
                f'font-family="sans-serif" font-size="11">{n}</text>'
            )
    for i, config in enumerate(CONFIGS):
        chunks.append(
            f'<text x="{30+i*70}" y="345" fill="{colors[config]}" '
            f'font-family="sans-serif" font-size="13">{config}</text>'
        )
    chunks.append("</svg>")
    path.write_text("\n".join(chunks) + "\n", encoding="utf-8")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cheap-model")
    ap.add_argument("--strong-model")
    ap.add_argument("--planner-model")
    ap.add_argument("--worker-model")
    ap.add_argument("--offline", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
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
    args.output.mkdir(parents=True, exist_ok=True)
    jsonl = args.output / "runs.jsonl"
    rows = []
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
                    row = run_one(argv2, config=config, n=n, repetition=repetition)
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
    print(f"\nresults: {args.output} ({len(rows)} runs)")
    return 0 if rows and all(r.get("status") == "PASSED" for r in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
