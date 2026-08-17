"""Live provider smoke — the M2 gate's one mandatory step that cannot run without a vendor key.

    python scripts/live_smoke.py --worker <provider>:<model> [--planner <provider>:<model>] [--step ping|worker|planner|repair|all]

Runs, in order, against the REAL provider (each step is a gate — the next one runs only if the previous passed):

  1. ping     one metered call through `mas models --ping --spec <worker>` (telemetry row, priced or flagged unpriced)
  2. worker   the URL-shortener benchmark with LLM workers (`--agent llm`), sandboxed command tools and the real
              acceptance verifier: hand-written DAG, real code written by the model, PASS required
  3. planner  goal → LLM planner proposes the acceptance contract → the operator approves it (this script prints the
              proposal and approves it for you unless --no-auto-approve, in which case it waits for `mas approve` from
              another terminal — the human decision stays a human decision) → planner produces the DAG → LLM workers →
              real verifier → PASS required. Bounded repair is on (--max-replans 1).
  4. repair   induced verifier failure → live repair → PASS: the hand-written DAG again, but the FIRST verification is
              replaced by an induced FAIL naming one real check id (the real verifier ran; its PASS is withheld once);
              the LLM planner must propose an amendment, LLM workers execute it, the second verification is the real
              one → PASS with replans_used == 1. (The failure is induced; the amendment and the re-verification are not.)

Model specs come ONLY from --worker/--planner or MAS_MODEL_WORKER/MAS_MODEL_PLANNER (model names never live in code
outside mas/providers/ and config). Prices come from MAS_MODEL_PRICES; unpriced usage is flagged, never hidden.
The key stays in this host process's environment (ANTHROPIC_API_KEY / MAS_OPENAI_API_KEY): never on the command line,
never in compose YAML; workers here are in-process threads, so no other process ever sees it.

Budgets are hard: --max-tokens / --max-cost-usd / --max-wallclock-s end a run with a verdict (I-4). Defaults are meant
for a smoke, not a benchmark. Exit code 0 only when every requested step passed. Everything the run did is on record:
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

GOAL = (
    "Build a small URL-shortener HTTP service in Python (standard library only, no third-party packages): "
    'POST /shorten with a JSON body {"url": ...} returns 201 and a short code; GET /<code> redirects (302) to the '
    "original URL; GET /stats reports the number of stored URLs; stored URLs survive a service restart (persist under "
    "the directory given by the STATE_DIR environment variable); include the service's own tests (pytest)."
)


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
        print("WARNING: MAS_MODEL_PRICES is empty — usage will be recorded but flagged unpriced (the cost budget cannot bite)")
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
    _hr(f"1/4 ping  {args.worker}")
    rc = cli.main(["models", "--ping", "--spec", args.worker])
    return rc == 0


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
    id marked failed — a deterministic, visible induction of the repair path (evaluation §5.3 / A8, live)."""

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


def _execute(conn, run_id, *, args: argparse.Namespace, planner, verifier=None) -> RunStatus:
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
    return final.status


def step_worker(args: argparse.Namespace) -> bool:
    _hr(f"2/4 worker  benchmarks/url_shortener/dag.json  workers={args.workers}  model={args.worker}")
    conn = connect()
    dag = DagSpec.from_file(str(ROOT / "benchmarks" / "url_shortener" / "dag.json"))
    run = runs_mod.create_run_from_dag(
        conn, dag, budgets=_budgets(args), capabilities=set(settings().worker_capabilities), pool=f"live:{os.getpid()}"
    )
    print(f"run {run.id}  ({len(dag.tasks)} tasks)")
    return _execute(conn, run.id, args=args, planner=None) is RunStatus.PASSED


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
                while runs_mod.sm.get_run(conn, run.id).status is RunStatus.AWAITING_INPUT:
                    time.sleep(2)
            else:
                frozen = contract_mod.approve(conn, run.id, acceptance_root=settings().acceptance_root, approved_by="live_smoke")
                print(f"\napproved (auto; --no-auto-approve to decide yourself): benchmark={frozen.benchmark}")
                print(f"suite_sha256={frozen.suite_sha256}")
        else:
            qs = runs_mod.pending_questions(conn, run.id)
            print(f'\nthe planner asked: {qs}\nanswer with: mas answer {run.id} "..."  (waiting)')
            while runs_mod.sm.get_run(conn, run.id).status is RunStatus.AWAITING_INPUT:
                time.sleep(2)
    # the orchestrator loop plans further rounds (DAG, any repair amendments) and executes
    return _execute(conn, run.id, args=args, planner=planner) is RunStatus.PASSED


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
    status = _execute(conn, run.id, args=args, planner=planner, verifier=verifier)
    final = runs_mod.sm.get_run(conn, run.id)
    ok = status is RunStatus.PASSED and final.replans_used == 1 and verifier.calls == 2
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
    ap.add_argument("--max-cost-usd", type=float, default=10.0)
    ap.add_argument("--max-wallclock-s", type=int, default=1800)
    ap.add_argument("--max-attempt-runtime-s", type=int, default=600)
    ap.add_argument("--max-replans", type=int, default=1)
    ap.add_argument(
        "--no-auto-approve", action="store_true", help="wait for `mas approve` instead of approving the proposal here"
    )
    ap.add_argument("--dry-run", action="store_true", help="preflight only")
    args = ap.parse_args(argv)

    problems = _preflight(args)
    if problems:
        print("\npreflight failed:\n  - " + "\n  - ".join(problems))
        return 2
    print(f"worker={args.worker} planner={args.planner or '(not needed)'} step={args.step}")
    print(
        f"budgets: tokens={args.max_tokens} attempt_tokens={args.max_attempt_tokens} cost=${args.max_cost_usd} "
        f"wallclock={args.max_wallclock_s}s"
    )
    if args.dry_run:
        return 0
    results: dict[str, bool] = {}
    order = ["ping", "worker", "planner", "repair"] if args.step == "all" else [args.step]
    for step in order:
        ok = {"ping": step_ping, "worker": step_worker, "planner": step_planner, "repair": step_repair}[step](args)
        results[step] = ok
        print(f"\n--> {step}: {'PASS' if ok else 'FAIL'}")
        if not ok:
            break
    _hr("live smoke summary")
    for k, v in results.items():
        print(f"  {k:8s} {'PASS' if v else 'FAIL'}")
    return 0 if results and all(results.values()) and len(results) == len(order) else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
