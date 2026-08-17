"""Run the frozen M3 A/B/C/D x N matrix and write machine-readable evidence plus a compact SVG report.

Real evaluation (minimum five repetitions per cell):
  python scripts/benchmark.py --cheap-model openai:... --strong-model anthropic:... \
      --planner-model anthropic:... --worker-model openai:... --repeats 5

Key-less substrate rehearsal (hand-written width DAG, stub agents, real verifier):
  python scripts/benchmark.py --offline --repeats 1

Every cell uses the same goal, acceptance suite and total budgets. A/B are produced by the runtime's single-agent
policy; C/D use the same planner/worker models and differ only in concurrency. JSONL is appended after each run so an
interrupted experiment remains auditable. Unpriced calls make cost null in the summary rather than falsely cheap.

Failure classification (`classify_run`): a run that ends PASSED, or FAILED/ABORTED because the model, the plan or the
experiment's budgets did (`experimental`), is evidence and stays. A run that ends because the machinery did — verifier
crash/timeout, unusable suite, provider outage, sandbox/workspace failure, a client that could not read the run — is
`infrastructure`: it cannot answer the MAS value question, it is kept in the append-only log for audit, and its cell is
rerun on the next invocation. Completion counts only the last row per cell/repetition, and only if every earlier row for
that key was infrastructure-invalid (a valid row is never rerun; two valid rows for one key are a duplicate).
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
from typing import Any
from uuid import UUID, uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from mas.config import settings  # noqa: E402
from mas.db import connect  # noqa: E402
from mas.evaluation import CONFIGS, WIDTHS, width_dag, width_goal  # noqa: E402
from mas.metrics import compute  # noqa: E402

RUN_ID = re.compile(r"\brun ([0-9a-f-]{36})\b", re.I)
TERMINAL = {"PASSED", "FAILED", "ABORTED"}
FAILURE_CLASSES = ("pass", "experimental", "infrastructure")
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
    budget, NO_PROGRESS, token/cost/wall-clock abort, invalid plan, policy denial, an agent that could not do the task).
    Infrastructure: the run never got a fair chance (verifier crash/timeout/unusable suite, missing planner, a task whose
    every failed attempt was a workspace/sandbox/provider/agent-crash failure, a client that lost the run).
    """
    status = row.get("status")
    if status == "PASSED":
        return "pass"
    if status not in TERMINAL:
        return "infrastructure"  # CLIENT_ERROR, or a run the client left non-terminal
    verdict = str(row.get("verdict") or "")
    reason = row.get("verdict_reason")
    if status == "ABORTED":
        # budgets are the experiment's parameters; anything else that aborts a run is not the model's doing
        return "experimental" if reason in (None, "BUDGET_EXHAUSTED") else "infrastructure"
    if reason == "UNSUPPORTED" or any(marker in verdict for marker in _INFRASTRUCTURE_VERDICT_MARKERS):
        return "infrastructure"
    if reason == "UNRECOVERABLE_FAILURE":
        # a task exhausted its attempts: the model's failure unless *every* failed attempt was the machinery's
        classes = row.get("attempt_failure_classes") if isinstance(row.get("attempt_failure_classes"), dict) else {}
        blamed = {k: int(v) for k, v in classes.items() if int(v) > 0 and k not in ("abandoned", "cancelled")}
        if blamed and set(blamed) <= {"infrastructure"}:
            return "infrastructure"
        return "experimental"
    return "experimental"  # NO_PROGRESS, BUDGET_EXHAUSTED, INVALID_PLAN, POLICY_DENIED


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


def render_analysis(summary: list[dict], manifest: dict, done: dict) -> str:
    """The M3 report (evaluation.md §4/§7) as Markdown. Deterministic in (summary, manifest, done): the MVP gate
    regenerates it from the raw rows and requires the file on disk to match byte for byte."""
    by = {(r["config"], r["n"]): r for r in summary}
    spec = manifest["spec"]
    widths, configs = list(spec["widths"]), list(spec["configs"])
    lines = [
        "# Frozen M3 result",
        "",
        f"Experiment: `{manifest['experiment_id']}`  ",
        f"Git commit: `{manifest['git_commit']}`  ",
        f"Mode: `{spec.get('mode')}`; models: `{json.dumps(spec.get('models'), sort_keys=True)}`  ",
        f"Budgets per run: `{json.dumps(spec.get('budgets'), sort_keys=True)}`  ",
        f"Evidence complete: **{done['evidence_complete']}** "
        f"({done.get('effective_runs', done['recorded_runs'])}/{done['expected_runs']} effective runs; "
        f"{done['recorded_runs']} rows recorded, {done.get('superseded_rows', 0)} infrastructure-invalid rows superseded by "
        f"reruns, {done.get('infrastructure_invalid_rows', 0)} still needing a rerun)  ",
        f"All runs passed: **{done['all_passed']}**",
        "",
        "Failure classification: `experimental` runs (the model, the plan or the run's budgets decided the outcome) are "
        "evidence and are kept; `infrastructure` runs (verifier crash/timeout, unusable suite, provider outage, "
        "sandbox/workspace failure, lost client) cannot answer the value question and are rerun. Success rate is over "
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


def write_analysis(summary: list[dict], manifest: dict, done: dict, path: Path) -> None:
    path.write_text(render_analysis(summary, manifest, done), encoding="utf-8")


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
    effective, _audit = effective_rows(rows)
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
                    row = run_one(argv2, config=config, n=n, repetition=repetition)
                    row["experiment_id"] = manifest["experiment_id"]
                    row["git_commit"] = manifest["git_commit"]
                    row["suite_sha256"] = manifest["spec"]["suite_sha256"][str(n)]
                    if previous is not None:
                        row["rerun_of"] = previous.get("run_id")
                        row["rerun_index"] = int(previous.get("rerun_index") or 0) + 1
                    rows.append(row)
                    effective[key] = row
                    with jsonl.open("a", encoding="utf-8") as f:
                        f.write(json.dumps(row, default=str) + "\n")
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
    write_analysis(summary, manifest, done, args.output / "analysis.md")
    print(
        f"\nresults: {args.output} ({done['effective_runs']}/{done['expected_runs']} effective runs, {len(rows)} rows; "
        f"evidence_complete={done['evidence_complete']}; all_passed={done['all_passed']}; "
        f"failure_classes={done['failure_classes']}; needing_rerun={len(done['keys_needing_rerun'])})"
    )
    if done["keys_needing_rerun"]:
        print("infrastructure-invalid cells will be rerun on the next invocation with the same arguments")
    # A model/task failure is valid experimental data. Missing/duplicate/infrastructure/unpriced evidence is not.
    return 0 if done["evidence_complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
