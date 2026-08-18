"""Live provider smoke - the M2 gate's one mandatory step that cannot run without a vendor key.

    python scripts/live_smoke.py --worker <provider>:<model> [--planner <provider>:<model>]
        [--step ping|worker|planner|repair|all]

Runs, in order, against the REAL provider (each step is a gate - the next one runs only if the previous passed):

  1. ping     one metered call through `mas models --ping --spec <worker>` (telemetry row, priced or flagged unpriced)
  2. worker   the URL-shortener benchmark with LLM workers (`--agent llm`), sandboxed command tools and the real
              acceptance verifier: hand-written DAG, real code written by the model, PASS required
  3. planner  goal -> LLM planner proposes the acceptance contract -> the operator approves it (this script prints the
              proposal and approves it for you unless --no-auto-approve, in which case it waits for `mas approve` from
              another terminal - the human decision stays a human decision) -> planner produces the DAG -> LLM workers ->
              real verifier -> PASS required. Bounded repair is on (--max-replans 1).
  4. repair   induced verifier failure -> live repair -> PASS: the hand-written DAG again, but the FIRST verification is
              replaced by an induced FAIL naming one real check id (the real verifier ran; its PASS is withheld once);
              the LLM planner must propose an amendment, LLM workers execute it, the second verification is the real
              one -> PASS with replans_used == 1. (The failure is induced; the amendment and the re-verification are not.)

Model specs come ONLY from --worker/--planner or MAS_MODEL_WORKER/MAS_MODEL_PLANNER (model names never live in code
outside mas/providers/ and config). Prices come from MAS_MODEL_PRICES; unpriced usage is flagged, never hidden.
The key stays in this host process's environment (ANTHROPIC_API_KEY / MAS_OPENAI_API_KEY): never on the command line,
never in compose YAML; workers here are in-process threads, so no other process ever sees it.

Run it **one stage at a time** and inspect between stages — a stage that exits 0 authorizes the next one, it does not
start it:

    live_smoke.py --worker <p:m> --planner <p:m> --no-auto-approve --max-cost-usd X --max-total-cost-usd Y \
        --output mvp-evidence/live-smoke.json --step ping
    ... same arguments ...  --step worker  --resume
    ... same arguments ...  --step planner --resume
    ... same arguments ...  --step repair  --resume

Every stage accumulates into one evidence file: passed stages, their runs and the paid ping are carried forward, and
`complete` turns true only when all four have passed. The identity arguments (both models, approval mode, budgets,
prices, request shape) must be **identical from the first stage on**, including `--planner` and `--no-auto-approve` on
the ping — resume refuses otherwise, and correctly: it would be evidence from a different experiment.

Budgets are hard: --max-tokens / --max-cost-usd / --max-wallclock-s end a run with a verdict (I-4). Defaults are meant
for a smoke, not a benchmark. `--max-total-cost-usd` bounds the WHOLE smoke on top of that (ADR-010): spend is summed
from the metered telemetry of the stages already run, a stage may only start if the ceiling still covers its own
per-run maximum, and every stage prints what it cost — this is the run that tells you what a real run costs, so it
says so out loud. Exit code 0 only when every requested step passed. Everything the run did is on record:
`mas status <run>`, `mas replay <run>`, `mas artifacts <run>`.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from mas import cli, metrics  # noqa: E402
from mas.config import settings  # noqa: E402
from mas.db import connect, migrate  # noqa: E402
from mas.models.enums import RunStatus  # noqa: E402
from mas.models.types import Budgets  # noqa: E402
from mas.orchestrator import runs as runs_mod  # noqa: E402
from mas.orchestrator import scheduler  # noqa: E402
from mas.planner import contracts as contract_mod  # noqa: E402
from mas.planner.dag import DagSpec  # noqa: E402
from mas.workers.runtime import Worker, run_worker_thread  # noqa: E402

ALL_STEPS = ("ping", "worker", "planner", "repair")  # the complete gate; `complete` means all four, however they ran
GOAL = (
    "Build a small URL-shortener HTTP service in Python (standard library only, no third-party packages): "
    'POST /shorten with a JSON body {"url": ...} returns 201 and a short code; GET /<code> redirects (302) to the '
    "original URL; GET /stats reports the number of stored URLs; stored URLs survive a service restart (persist under "
    "the directory given by the STATE_DIR environment variable); include the service's own tests (pytest)."
)


def _git_state() -> dict[str, object]:
    rev = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=False)
    dirty = subprocess.run(["git", "status", "--porcelain"], cwd=ROOT, capture_output=True, text=True, check=False)
    return {"commit": rev.stdout.strip() if rev.returncode == 0 else "unknown", "dirty": bool(dirty.stdout.strip())}


def _write_evidence(args: argparse.Namespace) -> None:
    """Atomic, and retried: this file is written after every stage of a paid run, and on Windows the rename can lose a
    brief race with a scanner holding the previous version (`PermissionError: [WinError 5]`). Losing a stage's evidence
    to an antivirus mid-scan would mean paying for it again."""
    if args.output is None:
        return
    args.output.parent.mkdir(parents=True, exist_ok=True)
    tmp = args.output.with_suffix(args.output.suffix + ".tmp")
    tmp.write_text(json.dumps(args.evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for attempt in range(6):
        try:
            tmp.replace(args.output)
            return
        except PermissionError:
            if attempt == 5:
                raise
            time.sleep(0.1 * (attempt + 1))


# evidence fields that must be identical for an earlier file's passed stages to count towards this invocation: the
# commit and tree state, the models, the approval mode, the pricing rule AND the price table, and the whole
# experimental setup (every budget, worker count, concurrency, repair limit) — a stage passed under other conditions
# is different evidence
_RESUME_IDENTITY = ("git", "models", "manual_contract_approval", "allow_unpriced", "model_prices", "setup", "request_shape")


def _price_snapshot() -> object:
    """The price table as configured (parsed when it is JSON, verbatim otherwise) — part of the resume identity."""
    prices = settings().model_prices.strip()
    try:
        return json.loads(prices) if prices else {}
    except json.JSONDecodeError:
        return prices


def _request_shape() -> dict:
    """How calls are made — thinking, reasoning effort, timeout and retries (ADR-010).

    Part of the resume identity because it changes what a stage costs and how it behaves: a stage that passed at one
    effort is not evidence for another, and this smoke's whole purpose is to measure the cost of the shape the matrix
    will then freeze."""
    s = settings()
    return {
        "anthropic_thinking": s.anthropic_thinking,
        "anthropic_effort": s.anthropic_effort,
        "anthropic_fallbacks": s.anthropic_fallbacks,
        "provider_timeout_s": s.provider_timeout_s,
        "provider_max_retries": s.provider_max_retries,
        "attempt_max_calls": s.attempt_max_calls,
    }


def _setup(args: argparse.Namespace) -> dict:
    """Everything about the experimental setup that decides how a stage runs (identical for a resume to count) —
    including the ping's own output ceiling, which decides what that stage costs."""
    return {"workers": args.workers, "ping_max_tokens": args.ping_max_tokens, **asdict(_budgets(args))}


def _charge(args: argparse.Namespace, *, step: str, kind: str, cost_usd: float | None, priced: bool, **extra) -> dict:
    """Append one paid operation to the **append-only ledger** and persist the evidence immediately.

    Money spent and stage qualified are different facts. A planner attempt that costs $5 and then fails still cost $5:
    if resume rebuilt the total from the stages that *passed*, that $5 would vanish and the operator could retry past
    the ceiling they set (ADR-010). So every billable operation lands here as it happens — successes, failures,
    infrastructure errors, retries, the ping — and nothing is ever removed. `steps`/`runs` answer "is this stage done";
    only this ledger answers "what has this cost". Writing it before the caller records the stage outcome also means a
    crash between the two loses the flag, never the charge."""
    entry = {
        "at": datetime.now(UTC).isoformat(),
        "step": step,
        "kind": kind,  # ping | run
        "cost_usd": cost_usd,
        "priced": bool(priced),
        **extra,
    }
    args.evidence.setdefault("ledger", []).append(entry)
    args.evidence["spend"] = _spend(args)
    _write_evidence(args)
    return entry


def _spend(args: argparse.Namespace) -> dict:
    """What this smoke has been billed, computed from the whole ledger — every attempt, not only the ones that counted.

    An operation with unpriced calls makes the total a **floor**: it is reported separately and never folded in as
    zero, and its stage's per-stage figure becomes null rather than a number that understates."""
    billed, unpriced, by_step = 0.0, 0, {}
    for entry in args.evidence.get("ledger", []):
        step = entry.get("step")
        if entry.get("priced") and entry.get("cost_usd") is not None:
            billed += float(entry["cost_usd"])
            if by_step.get(step, 0.0) is not None:
                by_step[step] = round(by_step.get(step, 0.0) + float(entry["cost_usd"]), 8)
        else:
            unpriced += int(entry.get("unpriced_calls") or 1)
            by_step[step] = None  # unknown cost in this stage: no honest number to show
    return {
        "billed_usd": round(billed, 6),
        "unpriced_calls": unpriced,
        "ceiling_usd": args.max_total_cost_usd,
        "operations": len(args.evidence.get("ledger", [])),
        "by_step": by_step,
    }


def _admit(args: argparse.Namespace, step: str) -> str | None:
    """Print what this stage may cost against the ceiling; return the reason it must not start (ADR-010)."""
    spend = _spend(args)
    cap, billed = args.max_total_cost_usd, spend["billed_usd"]
    print(
        f"[spend] billed ${billed:.4f} of ${cap:.4f} ceiling; {step} <= ${args.max_cost_usd:.4f} per run"
        + (f"; {spend['unpriced_calls']} unpriced call(s) so far" if spend["unpriced_calls"] else "")
    )
    if step == "ping":
        # the ping's cost cannot be bounded in USD before it runs (one call of --ping-max-tokens at an unknown price),
        # so it is admitted while any ceiling remains and *billed* afterwards like everything else; the next stage then
        # meets the ordinary rule. The ceiling is never bypassed, only crossed by at most one small call.
        return None if billed < cap else f"the ${cap:.4f} smoke ceiling is already spent (billed ${billed:.4f})"
    if billed + args.max_cost_usd > cap:
        return (
            f"the ${cap:.4f} smoke ceiling does not cover another ${args.max_cost_usd:.4f} run (billed ${billed:.4f}); "
            "raise --max-total-cost-usd deliberately to continue"
        )
    return None


def merge_resume(previous: dict, current: dict) -> tuple[dict, list[str], str | None]:
    """Carry an earlier evidence file's PASSED stages (and their runs) into `current`.

    Returns (merged evidence, stages of *this* invocation that need not run again, refusal reason). Everything that
    passed before is carried regardless of what this invocation requests — the stages accumulate across separate
    commands, which is how the paid sequence is meant to be run. Nothing is carried over unless the earlier file was
    produced by the same commit with a clean/dirty state, the same worker/planner models, the same approval mode, the
    same pricing rule and price table, and the same setup (budgets, workers, concurrency, replans) — otherwise a paid
    stage from a different setup could be smuggled into this gate."""
    if not isinstance(previous, dict) or previous.get("schema") != current.get("schema"):
        return current, [], "earlier evidence has a different schema"
    for key in _RESUME_IDENTITY:
        if previous.get(key) != current.get(key):
            return current, [], f"earlier evidence differs in {key}: {previous.get(key)!r} vs {current.get(key)!r}"
    prev_steps = previous.get("steps") if isinstance(previous.get("steps"), dict) else {}
    prev_runs = previous.get("runs") if isinstance(previous.get("runs"), list) else []
    # EVERY stage that already passed is carried, not only the ones this invocation asks for. Otherwise the documented
    # one-stage-at-a-time sequence (`--step ping`, then `--step worker --resume`, ...) would delete the evidence of
    # each paid stage as it moved to the next. `skip` is a different question — which of the *requested* stages need
    # not run again — and only that one depends on `requested_steps`. A failed stage is never carried as passed.
    carried = [s for s in ALL_STEPS if prev_steps.get(s) is True]
    skip = [s for s in current["requested_steps"] if prev_steps.get(s) is True]
    merged = dict(current)
    merged["steps"] = {s: True for s in carried}
    merged["runs"] = [r for r in prev_runs if isinstance(r, dict) and r.get("step") in carried]
    # The ledger is carried WHOLE — every earlier charge, including the failed and abandoned stages whose runs are not
    # carried. Qualification is per stage; spend is cumulative and irreversible (ADR-010). Dropping a failed stage's
    # cost here is how a retry would quietly exceed the ceiling the operator set.
    merged["ledger"] = [dict(e) for e in (previous.get("ledger") or []) if isinstance(e, dict)]
    # the ping's stage evidence travels with its stage; its *charge* is in the ledger either way
    if "ping" in carried and isinstance(previous.get("ping"), dict):
        merged["ping"] = previous["ping"]
    else:
        merged.pop("ping", None)
    merged["resumed_from"] = {
        "started_at": previous.get("started_at"),
        "steps": carried,
        "charges_carried": len(merged["ledger"]),
    }
    return merged, skip, None


def _hr(title: str) -> None:
    print("\n" + "=" * 100 + f"\n{title}\n" + "=" * 100, flush=True)


def _preflight(args: argparse.Namespace) -> list[str]:
    problems: list[str] = []
    keys = [k for k in ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "MAS_OPENAI_API_KEY", "OPENAI_API_KEY") if os.environ.get(k)]
    print(f"key variables present in this process: {keys or 'NONE'}")
    if not keys:
        problems.append("no vendor key in this process's environment (inject it into the shell that runs this script)")
    if not args.worker:
        problems.append("no worker model: --worker <provider>:<model> or MAS_MODEL_WORKER")
    if args.step in ("planner", "repair", "all") and not args.planner:
        problems.append("no planner model: --planner <provider>:<model> or MAS_MODEL_PLANNER (or --step worker)")
    if not settings().model_prices:
        message = "MAS_MODEL_PRICES is empty - cost is unknown and the cost budget cannot be enforced"
        if args.allow_unpriced:
            print(f"WARNING: {message}")
        else:
            problems.append(message + " (or explicitly use --allow-unpriced for a non-completion rehearsal)")
    shape = _request_shape()
    print("request shape: " + json.dumps(shape, sort_keys=True))
    if any(str(m or "").startswith("anthropic:") for m in (args.worker, args.planner)) and not shape["anthropic_effort"]:
        # this smoke measures the cost of the shape the M3 manifest then freezes (ADR-010)
        problems.append("MAS_ANTHROPIC_EFFORT is not set: the provider default would decide cost and behavior")
    if args.max_total_cost_usd < args.max_cost_usd:
        problems.append(
            f"--max-total-cost-usd {args.max_total_cost_usd} is below the per-run --max-cost-usd {args.max_cost_usd}: "
            "no stage could start"
        )
    d = subprocess.run(["docker", "info"], capture_output=True, timeout=30, check=False)
    if d.returncode != 0:
        problems.append("Docker daemon unreachable (sandboxed command tools and the real verifier need it)")
    img = subprocess.run(["docker", "image", "inspect", settings().verifier_image], capture_output=True, check=False)
    if img.returncode != 0:
        image = settings().verifier_image
        problems.append(f"verifier image {image} not built: docker build -f acceptance/Dockerfile.verifier -t {image} .")
    try:
        c = connect()
        migrate(c)
        c.close()
    except Exception as e:  # noqa: BLE001
        problems.append(f"Postgres unreachable ({e}); docker compose up -d postgres")
    return problems


def step_ping(args: argparse.Namespace) -> bool:
    """One metered call — and it is charged to this smoke's ledger like every other call (ADR-010).

    It is small (one call, `--ping-max-tokens`), but "small" is not "unaccounted": an unpriced ping means the price
    table does not cover the model the provider reported, which is exactly the thing to discover before the worker
    stage, not after."""
    _hr(f"1/4 ping  {args.worker}")
    record = cli.ping_spec(args.worker, role="worker", max_tokens=args.ping_max_tokens)
    args.evidence["ping"] = record
    if record["calls"]:  # a call that reached the provider is billable whether or not the stage qualifies
        _charge(
            args,
            step="ping",
            kind="ping",
            cost_usd=record["cost_usd"],
            priced=record["priced"],
            models=record["models"],
            calls=len(record["calls"]),
            ok=record["ok"],
            unpriced_calls=sum(1 for c in record["calls"] if not c.get("priced")),
        )
    if record["ok"]:
        print(f"{args.worker}: {record.get('stop_reason')} {record.get('text', '')!r}")
    else:
        print(f"{args.worker}: FAILED {record['error']}", file=sys.stderr)
    for call in record["calls"]:
        print("   ", json.dumps(call, default=str))
    if record["models"]:
        print(f"  provider reported model(s): {record['models']}  priced={record['priced']}")
    if not record["priced"]:
        print(
            "  the ping is unpriced: MAS_MODEL_PRICES does not cover the model the provider reported "
            f"({record['models'] or 'unknown'})",
            file=sys.stderr,
        )
    return bool(record["ok"]) and (record["priced"] or args.allow_unpriced)


def _budgets(args: argparse.Namespace) -> Budgets:
    return Budgets(
        max_concurrency=args.max_concurrency,
        lease_s=15,
        max_wallclock_s=args.max_wallclock_s,
        max_attempt_runtime_s=args.max_attempt_runtime_s,
        max_tokens=args.max_tokens,
        max_attempt_tokens=args.max_attempt_tokens,
        max_cost_usd=args.max_cost_usd,
        max_replans=args.max_replans,
        max_attempts_per_task=2,
    )


class InducedFirstFail:
    """Wraps the real verifier: runs it every time, but withholds the first verdict and returns FAIL with one real check
    id marked failed - a deterministic, visible induction of the repair path (evaluation section 5.3 / A8, live)."""

    name = "induced-first-fail"

    def __init__(self, inner):
        self.inner = inner
        self.calls = 0

    def verify(self, request):
        from mas.verifier.base import CheckResult, CheckStatus, VerificationResult, VerificationStatus

        self.calls += 1
        real = self.inner.verify(request)
        if self.calls > 1 or real.status is not VerificationStatus.PASS:
            return real
        checks = list(real.checks)
        if not checks:
            return VerificationResult.fail("induced failure (no checks to name)", status=VerificationStatus.FAIL)
        first, rest = checks[0], checks[1:]
        induced = CheckResult(first.id, CheckStatus.FAIL, f"INDUCED by live_smoke: {first.detail or 'treated as failing'}")
        return VerificationResult.fail(
            "induced failure: one real check reported as failed (the real run passed; verdict withheld once)",
            status=VerificationStatus.FAIL,
            checks=(induced, *rest),
            evidence={**real.evidence, "induced": True},
        )


def _record_run(args: argparse.Namespace, run_id, *, step: str, status: str, metrics_obj, verdict=None, verdict_reason=None):
    """One executed run: its charge in the ledger, its outcome in `runs`. Both, whatever the verdict — a run that
    failed or was aborted still called the model, and both facts are evidence."""
    priced = metrics_obj.unpriced_calls == 0
    _charge(
        args,
        step=step,
        kind="run",
        cost_usd=metrics_obj.call_cost_usd if priced else None,
        priced=priced,
        run_id=str(run_id),
        status=status,
        verdict_reason=verdict_reason,
        model_calls=metrics_obj.model_calls,
        unpriced_calls=metrics_obj.unpriced_calls,
    )
    args.evidence["runs"].append(
        {
            "step": step,
            "run_id": str(run_id),
            "status": status,
            "verdict": verdict,
            "verdict_reason": verdict_reason,
            "priced": priced,
            "metrics": metrics_obj.as_dict(),
        }
    )
    _write_evidence(args)


def _execute(conn, run_id, *, args: argparse.Namespace, planner, evidence_step: str, verifier=None) -> tuple[RunStatus, bool]:
    """Workers (in-process threads, LLM agent, sandboxed command tools) + orchestrator loop with the real verifier."""
    caps = set(settings().worker_capabilities)
    provider = cli._worker_provider(args.worker)
    agent = cli._agent("llm", stub_sleep=0.0, provider=provider)
    ws = cli._workspace("git")
    stop = threading.Event()
    workers = [
        Worker(
            f"live-{i + 1}",
            sorted(caps),
            agent,
            poll_s=0.2,
            run_id=run_id,
            workspace=ws,
            provider=provider,
            exec_backend_factory=cli._exec_backend_factory("sandbox", worker_id=f"live-{i + 1}"),
        )
        for i in range(args.workers)
    ]
    threads = [run_worker_thread(w, stop) for w in workers]
    t0 = time.monotonic()
    try:
        final = scheduler.run_until_terminal(
            conn,
            run_id,
            verifier=verifier or cli._acceptance_verifier(),
            planner=planner,
            capabilities=caps,
            workspace=ws,
            tick_s=0.5,
            timeout_s=args.max_wallclock_s + 120,
        )
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=10)
        scheduler.gc_workspace(run_id, ws)
    m = metrics.compute(conn, run_id)
    reason = f"  reason={final.verdict_reason}" if final.verdict_reason else ""
    print(f"\n{final.status.value}  verdict={final.verdict}{reason}  in {time.monotonic() - t0:.1f}s")
    cli._print_metrics(m)
    for w in workers:
        s = w.stats
        print(f"  {w.worker_id}: claimed={s.claimed} completed={s.completed} failed={s.failed} stale={s.stale}")
    print(f"  mas status {run_id} | mas replay {run_id} | mas artifacts {run_id}")
    priced = m.unpriced_calls == 0
    # charge FIRST: the money is spent whatever the verdict, and a crash before the stage flag is written must lose
    # the flag, never the charge
    _record_run(
        args,
        run_id,
        step=evidence_step,
        status=final.status.value,
        metrics_obj=m,
        verdict=final.verdict,
        verdict_reason=final.verdict_reason,
    )
    if not priced and not args.allow_unpriced:
        print("completion gate failed: this run contains unpriced model calls", file=sys.stderr)
    return final.status, priced or args.allow_unpriced


def step_worker(args: argparse.Namespace) -> bool:
    _hr(f"2/4 worker  benchmarks/url_shortener/dag.json  workers={args.workers}  model={args.worker}")
    conn = connect()
    dag = DagSpec.from_file(str(ROOT / "benchmarks" / "url_shortener" / "dag.json"))
    run = runs_mod.create_run_from_dag(
        conn, dag, budgets=_budgets(args), capabilities=set(settings().worker_capabilities), pool=f"live:{os.getpid()}"
    )
    print(f"run {run.id}  ({len(dag.tasks)} tasks)")
    status, priced = _execute(conn, run.id, args=args, planner=None, evidence_step="worker")
    return status is RunStatus.PASSED and priced


def _await_human(conn, run_id, *, planner, what: str, poll_s: float = 2.0):
    """Wait for `mas approve` / `mas answer` **on the run's clock** (I-4, ADR-006).

    Polling the row would let a forgotten approval wait forever: a run's wall-clock budget is only enforced when
    somebody ticks it, and nobody else is ticking this run. So the wait drives the canonical scheduler — the same
    `scheduler.tick` the orchestrator uses — and returns the moment the run leaves `AWAITING_INPUT`, including when
    its own budget ended it (`ABORTED` / `BUDGET_EXHAUSTED`). A human who never answers costs one wall-clock budget,
    not an unbounded process."""
    caps = set(settings().worker_capabilities)
    ws = cli._workspace("git")
    while True:
        run = scheduler.tick(conn, run_id, planner=planner, capabilities=caps, workspace=ws)
        if run.status is not RunStatus.AWAITING_INPUT:
            if run.status.terminal:
                print(f"\nthe run ended while waiting for {what}: {run.status.value} {run.verdict}", file=sys.stderr)
            return run
        time.sleep(poll_s)


def step_planner(args: argparse.Namespace) -> bool:
    _hr(f"3/4 planner  goal -> contract -> approve -> DAG -> workers -> verifier   planner={args.planner}")
    conn = connect()
    caps = set(settings().worker_capabilities)
    planner = cli._planner("llm", spec=args.planner)
    run = runs_mod.create_run(conn, goal=GOAL, budgets=_budgets(args), pool=f"live:{os.getpid()}")
    print(f"run {run.id}")
    run = runs_mod.plan_run(conn, run.id, planner, capabilities=caps)
    print(f"planner round 1 -> {run.status.value}")
    if run.status is RunStatus.AWAITING_INPUT:
        prop = contract_mod.pending_proposal(conn, run.id)
        if prop:
            print("\nproposed acceptance contract:")
            print(json.dumps(prop["proposal"], indent=2)[:6000])
            if args.no_auto_approve:
                print(f"\nwaiting for:  mas approve {run.id}   (from another terminal; the run's wall-clock keeps running)")
                run = _await_human(conn, run.id, planner=planner, what="`mas approve`")
                if run.status.terminal:
                    _record_run(
                        args,
                        run.id,
                        step="planner",
                        status=run.status.value,
                        metrics_obj=metrics.compute(conn, run.id),
                        verdict=run.verdict,
                        verdict_reason=run.verdict_reason,
                    )
                    return False
            else:
                frozen = contract_mod.approve(conn, run.id, acceptance_root=settings().acceptance_root, approved_by="live_smoke")
                print(f"\napproved (auto; --no-auto-approve to decide yourself): benchmark={frozen.benchmark}")
                print(f"suite_sha256={frozen.suite_sha256}")
        else:
            qs = runs_mod.pending_questions(conn, run.id)
            print(f'\nthe planner asked: {qs}\nanswer with: mas answer {run.id} "..."  (waiting)')
            run = _await_human(conn, run.id, planner=planner, what="`mas answer`")
            if run.status.terminal:
                _record_run(
                    args,
                    run.id,
                    step="planner",
                    status=run.status.value,
                    metrics_obj=metrics.compute(conn, run.id),
                    verdict=run.verdict,
                    verdict_reason=run.verdict_reason,
                )
                return False
    # the orchestrator loop plans further rounds (DAG, any repair amendments) and executes
    status, priced = _execute(conn, run.id, args=args, planner=planner, evidence_step="planner")
    return status is RunStatus.PASSED and priced


def step_repair(args: argparse.Namespace) -> bool:
    _hr(f"4/4 repair  induced verifier FAIL -> LLM amendment -> real re-verification   planner={args.planner}")
    conn = connect()
    dag = DagSpec.from_file(str(ROOT / "benchmarks" / "url_shortener" / "dag.json"))
    budgets = _budgets(args)
    if budgets.max_replans < 1:
        print("--max-replans must be >= 1 for the repair step")
        return False
    run = runs_mod.create_run_from_dag(
        conn, dag, budgets=budgets, capabilities=set(settings().worker_capabilities), pool=f"live:{os.getpid()}"
    )
    print(f"run {run.id}  ({len(dag.tasks)} tasks; the first verification will be an induced FAIL)")
    planner = cli._planner("llm", spec=args.planner)
    verifier = InducedFirstFail(cli._acceptance_verifier())
    status, priced = _execute(conn, run.id, args=args, planner=planner, evidence_step="repair", verifier=verifier)
    final = runs_mod.sm.get_run(conn, run.id)
    ok = status is RunStatus.PASSED and priced and final.replans_used == 1 and verifier.calls == 2
    verdict = "PASS" if ok else "FAIL"
    print(f"repair check: status={status.value} replans_used={final.replans_used} verifications={verifier.calls} -> {verdict}")
    return ok


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--worker", default=os.environ.get("MAS_MODEL_WORKER") or None, help="<provider>:<model> for workers")
    ap.add_argument("--planner", default=os.environ.get("MAS_MODEL_PLANNER") or None, help="<provider>:<model> for the planner")
    ap.add_argument("--step", choices=["ping", "worker", "planner", "repair", "all"], default="all")
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--max-concurrency", type=int, default=3)
    ap.add_argument("--max-tokens", type=int, default=1_500_000)
    ap.add_argument("--max-attempt-tokens", type=int, default=250_000)
    ap.add_argument("--max-cost-usd", type=float, default=10.0, help="per-run ceiling (three runs in a full smoke)")
    ap.add_argument(
        "--max-total-cost-usd",
        type=float,
        default=30.0,
        help="ceiling for the WHOLE smoke (ADR-010): a stage may only start if this still covers its per-run maximum",
    )
    ap.add_argument("--max-wallclock-s", type=int, default=1800)
    ap.add_argument("--max-attempt-runtime-s", type=int, default=600)
    ap.add_argument("--max-replans", type=int, default=1)
    ap.add_argument("--ping-max-tokens", type=int, default=64, help="output ceiling for the ping's single call")
    ap.add_argument(
        "--no-auto-approve", action="store_true", help="wait for `mas approve` instead of approving the proposal here"
    )
    ap.add_argument(
        "--output",
        type=Path,
        help="write incremental machine-readable gate evidence to this JSON file after every stage. An existing file is "
        "NEVER overwritten: continue it with --resume, or choose a new path for a separate experiment",
    )
    ap.add_argument(
        "--resume",
        action="store_true",
        help="carry every stage already PASSED in --output (with its runs and the paid ping) into this invocation, and "
        "skip any of them this command requests again. Only from the same commit, models, approval mode, pricing rule "
        "and price table, budgets, worker count, concurrency, replan limit and request shape — so repeat those "
        "identity arguments on every stage, including the first",
    )
    ap.add_argument(
        "--allow-unpriced",
        action="store_true",
        help="allow a rehearsal with unknown cost; evidence will not qualify as the priced MVP gate",
    )
    ap.add_argument("--dry-run", action="store_true", help="preflight only")
    args = ap.parse_args(argv)
    # Paid evidence is never replaced. Refused here — before the preflight, before any provider contact — because even
    # a *failed* preflight writes this file, which would erase an earlier paid result on the way to reporting an
    # unrelated problem. A separate experiment gets a separate path; a continuation gets --resume.
    if args.output is not None and args.output.exists() and not args.resume:
        ap.error(
            f"{args.output} already exists and holds paid evidence: continue it with --resume, or choose a new "
            "--output path for a separate experiment (evidence is never overwritten)"
        )
    order = list(ALL_STEPS) if args.step == "all" else [args.step]
    args.evidence = {
        "schema": 1,
        "started_at": datetime.now(UTC).isoformat(),
        "git": _git_state(),
        "models": {"worker": args.worker, "planner": args.planner},
        "requested_steps": order,
        "manual_contract_approval": bool(args.no_auto_approve),
        "allow_unpriced": bool(args.allow_unpriced),
        "model_prices": _price_snapshot(),
        "setup": _setup(args),
        "request_shape": _request_shape(),
        "steps": {},  # which stages qualified
        "runs": [],  # the runs behind the qualifying stages
        "ledger": [],  # append-only: EVERY billable operation, qualifying or not (ADR-010)
        "complete": False,
    }
    skip: list[str] = []
    if args.resume:
        if args.output is None:
            ap.error("--resume needs --output")
        if args.output.is_file():
            try:
                previous = json.loads(args.output.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                ap.error(f"cannot resume from {args.output}: {exc}")
            args.evidence, skip, refusal = merge_resume(previous, args.evidence)
            if refusal:
                print(f"not resuming: {refusal}")
            elif skip:
                print(f"resuming: skipping already-passed stages {skip} from {args.output}")
        else:
            print(f"nothing to resume: {args.output} does not exist yet")

    problems = _preflight(args)
    if problems:
        print("\npreflight failed:\n  - " + "\n  - ".join(problems))
        args.evidence["preflight_errors"] = problems
        args.evidence["finished_at"] = datetime.now(UTC).isoformat()
        _write_evidence(args)
        return 2
    print(f"worker={args.worker} planner={args.planner or '(not needed)'} step={args.step}")
    print(
        f"budgets: tokens={args.max_tokens} attempt_tokens={args.max_attempt_tokens} cost=${args.max_cost_usd}/run "
        f"(smoke ceiling ${args.max_total_cost_usd}) wallclock={args.max_wallclock_s}s"
    )
    if args.dry_run:
        args.evidence["preflight_only"] = True
        args.evidence["finished_at"] = datetime.now(UTC).isoformat()
        _write_evidence(args)
        return 0
    results: dict[str, bool] = {}
    stopped: str | None = None
    for step in order:
        if step in skip:
            results[step] = True
            print(f"\n--> {step}: PASS (carried forward from earlier evidence)")
            continue
        stopped = _admit(args, step)
        if stopped:
            print(f"\n[stop] {stopped}")
            break
        ok = {"ping": step_ping, "worker": step_worker, "planner": step_planner, "repair": step_repair}[step](args)
        results[step] = ok
        args.evidence["steps"][step] = ok
        args.evidence["spend"] = _spend(args)
        _write_evidence(args)
        print(f"\n--> {step}: {'PASS' if ok else 'FAIL'}  (billed so far ${args.evidence['spend']['billed_usd']:.4f})")
        if not ok:
            break
    _hr("live smoke summary")
    for k, v in results.items():
        print(f"  {k:8s} {'PASS' if v else 'FAIL'}" + ("  (resumed)" if k in skip else ""))
    spend = _spend(args)
    args.evidence["spend"] = spend
    print(
        f"\nspend: ${spend['billed_usd']:.4f} of the ${spend['ceiling_usd']:.4f} smoke ceiling; per stage "
        + json.dumps(spend["by_step"], sort_keys=True)
        + (f"; {spend['unpriced_calls']} unpriced call(s) (the total is a floor)" if spend["unpriced_calls"] else "")
    )
    print("use this to choose the matrix ceilings (--max-cost-usd per run, --max-total-cost-usd for 125 operations)")
    if stopped:
        args.evidence["stopped"] = stopped
    # Two different questions. `complete` is about the *evidence*: all four stages passed, whether in one command or in
    # four. The exit code is about *this* command, so a green `--step ping` authorizes the next stage without claiming
    # the gate is finished.
    requested_ok = bool(results) and all(results.values()) and len(results) == len(order)
    passed = [s for s in ALL_STEPS if args.evidence["steps"].get(s) is True]
    complete = len(passed) == len(ALL_STEPS)
    args.evidence["complete"] = complete
    args.evidence["finished_at"] = datetime.now(UTC).isoformat()
    _write_evidence(args)
    print(f"evidence: {len(passed)}/{len(ALL_STEPS)} stages passed {passed}; complete={complete}")
    if not complete and requested_ok:
        remaining = [s for s in ALL_STEPS if s not in passed]
        print(f"next: rerun with --step {remaining[0]} --resume and the SAME identity arguments (models, approval, budgets)")
    return 0 if requested_ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
