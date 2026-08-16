"""Deterministic control plane. No LLM calls in this package, ever (invariant I-1).

- state_machine.py  the ONLY place run/task/attempt/artifact statuses change (invariant I-2)
- leases.py         claim / heartbeat / reap / report
- budgets.py        hard-limit checks
- scheduler.py      per-run tick: reap → budgets → readiness → run progression → verifier stage
"""
