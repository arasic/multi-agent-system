"""Deterministic progress accounting for bounded repair (step 13-lite; ADR-008 §7). No model calls.

After every verifier FAIL the orchestrator computes a **failure fingerprint** from system-owned facts only:

  failing_checks     the acceptance check ids that did not pass (sorted)
  failure_classes    normalized failure classes: the verification status, each failing check's status and its detail
                     with volatile tokens (hex ids, paths, durations, numbers) folded — so "the same kind of failure"
                     compares equal while a *different* failure of the same check does not
  integration_hash   what was verified: the integration commit's tree hash (git workspace) — the diff, not the commit id,
                     so a repair that changed nothing observable repeats it — or the opaque ref otherwise
  accepted_artifacts hash over the run's accepted (type, ref) pairs at verification time

and the **amendment hash** of every installed amendment (structure + goals, task ids normalized), kept separately.
`NO_PROGRESS` is then a deterministic decision (`decide_after_fail`): a failure fingerprint that repeats an earlier one,
an amendment that repeats an earlier one, or the repair window closing without a reduction in failed criteria. An LLM
never declares that progress occurred; the driver never asks it.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any

from mas.models.enums import VerdictReason

_HEX = re.compile(r"\b[0-9a-f]{7,}\b")
_UUID = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b")
_PATH = re.compile(r"(?:[a-z]:)?(?:[\\/][\w.\-]+){2,}")
_DURATION = re.compile(r"\b\d+(?:\.\d+)?\s*(?:ms|s|sec|seconds?|min)\b")
_NUM = re.compile(r"\b\d+(?:\.\d+)?\b")
_WS = re.compile(r"\s+")


def normalize_detail(text: str, *, limit: int = 160) -> str:
    """Fold volatile tokens so equal *kinds* of failure compare equal: lowercase; uuids/hex ids, filesystem paths,
    durations and numbers become placeholders; whitespace collapsed; bounded."""
    t = (text or "").lower()
    t = _UUID.sub("<id>", t)
    t = _HEX.sub("<hex>", t)
    t = _PATH.sub("<path>", t)
    t = _DURATION.sub("<t>", t)
    t = _NUM.sub("<n>", t)
    t = _WS.sub(" ", t).strip()
    return t[:limit]


def canonical(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str)


def digest(obj: Any) -> str:
    return hashlib.sha256(canonical(obj).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Fingerprint:
    failing_checks: tuple[str, ...]
    failure_classes: tuple[str, ...]
    integration_hash: str
    accepted_artifacts: str
    verification_status: str

    @property
    def value(self) -> str:
        return digest(self.as_dict())

    def as_dict(self) -> dict[str, Any]:
        return {
            "failing_checks": list(self.failing_checks),
            "failure_classes": list(self.failure_classes),
            "integration_hash": self.integration_hash,
            "accepted_artifacts": self.accepted_artifacts,
            "verification_status": self.verification_status,
        }


def failure_fingerprint(
    report: dict[str, Any],
    *,
    integration_hash: str | None,
    accepted: list[tuple[str, str]],
) -> Fingerprint:
    """From a verification report (`VerificationResult.report`), the verified integration's tree hash / ref, and the
    run's accepted artifacts. Pure; the same facts always give the same fingerprint."""
    status = str(report.get("status") or "FAIL")
    checks = report.get("checks") or []
    failing = sorted(str(c.get("id")) for c in checks if isinstance(c, dict) and str(c.get("status", "")) != "PASS")
    classes = {f"verification:{status}"}
    for c in checks:
        if not isinstance(c, dict) or str(c.get("status", "")) == "PASS":
            continue
        classes.add(f"check:{c.get('id')}:{c.get('status')}:{normalize_detail(str(c.get('detail') or ''))}")
    if report.get("reason"):
        classes.add(f"reason:{normalize_detail(str(report['reason']))}")
    return Fingerprint(
        failing_checks=tuple(failing),
        failure_classes=tuple(sorted(classes)),
        integration_hash=integration_hash or "",
        accepted_artifacts=digest(sorted(accepted)),
        verification_status=status,
    )


def amendment_hash(tasks: list[dict[str, Any]]) -> str:
    """Structure + goals of an amendment with the new task ids normalized (a planner that re-proposes the same repair
    under new ids still repeats itself). Dependencies on existing tasks keep their keys (they are stable)."""
    rows = []
    for t in tasks:
        rows.append(
            {
                "capability": str(t.get("capability", "")),
                "goal": " ".join(str(t.get("goal", "")).split()).lower(),
                "depends_on": sorted(str(d) for d in t.get("depends_on", []) or []),
                "output_contract": t.get("output_contract") or {},
                "id": str(t.get("id")),
            }
        )
    rows.sort(key=lambda r: (r["capability"], r["goal"], r["depends_on"], r["id"]))
    alias = {r["id"]: f"${i}" for i, r in enumerate(rows)}
    for r in rows:
        r["id"] = alias[r["id"]]
        r["depends_on"] = sorted(alias.get(d, d) for d in r["depends_on"])
    return digest(rows)


@dataclass(frozen=True)
class RepairDecision:
    action: str  # "replan" | "fail"
    reason: VerdictReason | None = None
    detail: str = ""
    payload: dict[str, Any] = field(default_factory=dict)


def decide_after_fail(
    fp: Fingerprint,
    *,
    previous: list[dict[str, Any]],  # earlier fingerprints' as_dict()+{"value"} for this run, oldest first
    replans_used: int,
    max_replans: int,
) -> RepairDecision:
    """The bounded-repair decision after a verifier FAIL. Deterministic; nothing here consults a model.

    repeated fingerprint            → FAIL NO_PROGRESS (the repair changed nothing observable)
    replans_used >= max_replans     → FAIL BUDGET_EXHAUSTED — or NO_PROGRESS when at least one repair ran and the
                                      number of failed criteria never went below the first failure's
    otherwise                       → REPLANNING (one more repair cycle; the driver asks the planner for an amendment)
    """
    value = fp.value
    for j, prev in enumerate(previous):
        if prev.get("value") == value:
            return RepairDecision(
                "fail",
                VerdictReason.NO_PROGRESS,
                f"verification failed with the same progress fingerprint as cycle {j} "
                f"(failing checks {list(fp.failing_checks) or 'none'}; integration {fp.integration_hash[:12] or '?'})",
                {"repeat_of_cycle": j},
            )
    if replans_used >= max_replans:
        if previous:
            first = len(previous[0].get("failing_checks") or [])
            if len(fp.failing_checks) >= first:
                return RepairDecision(
                    "fail",
                    VerdictReason.NO_PROGRESS,
                    f"{replans_used} repair cycle(s) did not reduce the failed acceptance criteria "
                    f"({first} -> {len(fp.failing_checks)}); max_replans={max_replans}",
                )
            return RepairDecision(
                "fail",
                VerdictReason.BUDGET_EXHAUSTED,
                f"still failing after max_replans={max_replans} repair cycle(s) "
                f"(failed criteria {first} -> {len(fp.failing_checks)})",
            )
        return RepairDecision(
            "fail",
            VerdictReason.BUDGET_EXHAUSTED,
            f"verification failed and no repair budget remains (max_replans={max_replans})",
        )
    return RepairDecision("replan", None, f"repair cycle {replans_used + 1}/{max_replans}")
