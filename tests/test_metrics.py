"""Pure metrics helpers (no database): critical-path duration and attempt-failure classification."""

import pytest

from mas.metrics import ATTEMPT_FAILURE_CLASSES, classify_attempt_failure, critical_path_seconds


def test_critical_path_is_the_longest_weighted_dependency_chain():
    # diamond: T1 -> {T2, T3, T4} -> T5; the chain through the slowest middle task is the critical path
    durations = {"T1": 1.0, "T2": 2.0, "T3": 5.0, "T4": 3.0, "T5": 1.5}
    deps = {"T2": {"T1"}, "T3": {"T1"}, "T4": {"T1"}, "T5": {"T2", "T3", "T4"}}
    assert critical_path_seconds(durations, deps) == 7.5
    assert critical_path_seconds(durations, deps) < sum(durations.values())  # parallel work is not on the path


def test_critical_path_handles_independent_tasks_missing_durations_and_cycles():
    assert critical_path_seconds({"A": 2.0, "B": 3.0}, {}) == 3.0  # width-N: the slowest independent task
    assert critical_path_seconds({}, {}) == 0.0
    assert critical_path_seconds({"A": 2.0}, {"B": {"A"}}) == 2.0  # a node with no recorded attempts weighs 0
    # a back edge (impossible for a validated plan) is ignored instead of recursing forever
    assert critical_path_seconds({"A": 1.0, "B": 1.0}, {"A": {"B"}, "B": {"A"}}) == 2.0


@pytest.mark.parametrize(
    "status, reason, expected",
    [
        ("SUCCESS", None, None),
        ("RUNNING", None, None),
        ("ABANDONED", "lease expired", "abandoned"),
        ("TIMEOUT", "max_attempt_runtime_s", "timeout"),
        ("CANCELLED", "run aborted: budget", "cancelled"),
        ("FAILED", "attempt ended: budget exhausted (tokens)", "budget"),
        ("FAILED", "attempt ended: deadline", "timeout"),
        ("FAILED", "attempt ended: cancelled", "cancelled"),
        ("FAILED", "workspace: git worktree add failed", "infrastructure"),
        ("FAILED", "workspace publish: commit failed", "infrastructure"),
        ("FAILED", "agent crashed: RuntimeError('x')", "infrastructure"),
        ("FAILED", "llm agent crashed: KeyError: 'y'", "infrastructure"),
        ("FAILED", "model provider error: ProviderUnavailable: 529", "infrastructure"),
        ("FAILED", "llm agent: no model provider for this worker", "infrastructure"),
        ("FAILED", "sandbox: container died", "infrastructure"),
        ("FAILED", "model refused the task (provider stop_reason=refusal)", "model"),
        ("FAILED", "model output truncated 3 times (max_tokens)", "model"),
        ("FAILED", "model stopped without calling `finish`", "model"),
        ("FAILED", "2 malformed tool calls (invalid JSON arguments)", "model"),
        ("FAILED", "tool-call budget exhausted (40)", "model"),
        ("FAILED", "model reported failure: cannot satisfy contract", "model"),
        ("FAILED", "finish listed invalid artifacts: x", "model"),
        ("FAILED", None, "model"),
    ],
)
def test_attempt_failure_classification(status: str, reason: str | None, expected: str | None):
    result = classify_attempt_failure(status, reason)
    assert result == expected
    assert result is None or result in ATTEMPT_FAILURE_CLASSES
