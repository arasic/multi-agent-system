"""Pure unit tests for the transition tables (no DB). docs/architecture.md §4."""

from mas.models.enums import (
    RUN_TERMINAL,
    TASK_TERMINAL,
    ArtifactStatus,
    AttemptStatus,
    RunStatus,
    TaskStatus,
)
from mas.orchestrator.state_machine import (
    ARTIFACT_TRANSITIONS,
    ATTEMPT_TRANSITIONS,
    RUN_TRANSITIONS,
    TASK_TRANSITIONS,
    can_artifact,
    can_attempt,
    can_run,
    can_task,
)


def test_every_state_has_an_entry():
    assert set(RUN_TRANSITIONS) == set(RunStatus)
    assert set(TASK_TRANSITIONS) == set(TaskStatus)
    assert set(ATTEMPT_TRANSITIONS) == set(AttemptStatus)
    assert set(ARTIFACT_TRANSITIONS) == set(ArtifactStatus)


def test_terminal_states_have_no_exits():
    for s in RUN_TERMINAL:
        assert RUN_TRANSITIONS[s] == frozenset()
        assert s.terminal
    for s in TASK_TERMINAL:
        assert TASK_TRANSITIONS[s] == frozenset()
        assert s.terminal
    for s in AttemptStatus:
        if s is not AttemptStatus.RUNNING:
            assert ATTEMPT_TRANSITIONS[s] == frozenset()
            assert s.terminal


def test_non_terminal_states_can_always_reach_a_terminal_state():
    def reaches_terminal(table, start, terminal):
        seen, frontier = set(), [start]
        while frontier:
            s = frontier.pop()
            if s in terminal:
                return True
            for n in table[s]:
                if n not in seen:
                    seen.add(n)
                    frontier.append(n)
        return False

    for s in RunStatus:
        assert reaches_terminal(RUN_TRANSITIONS, s, RUN_TERMINAL), s
    for s in TaskStatus:
        assert reaches_terminal(TASK_TRANSITIONS, s, TASK_TERMINAL), s


def test_run_happy_path_and_guards():
    assert can_run(RunStatus.CREATED, RunStatus.PLANNING)
    assert can_run(RunStatus.PLANNING, RunStatus.RUNNING)
    assert can_run(RunStatus.RUNNING, RunStatus.VERIFYING)
    assert can_run(RunStatus.VERIFYING, RunStatus.PASSED)
    assert can_run(RunStatus.VERIFYING, RunStatus.REPLANNING)
    assert can_run(RunStatus.REPLANNING, RunStatus.RUNNING)
    # every non-terminal state can be aborted (budgets, I-4)
    for s in RunStatus:
        if not s.terminal:
            assert can_run(s, RunStatus.ABORTED), s
    assert not can_run(RunStatus.CREATED, RunStatus.RUNNING)  # must go through PLANNING
    assert not can_run(RunStatus.RUNNING, RunStatus.PASSED)  # must go through VERIFYING (I-3)
    assert not can_run(RunStatus.PASSED, RunStatus.RUNNING)


def test_task_paths():
    assert can_task(TaskStatus.PENDING, TaskStatus.READY)
    assert can_task(TaskStatus.PENDING, TaskStatus.BLOCKED)
    assert can_task(TaskStatus.READY, TaskStatus.RUNNING)
    assert can_task(TaskStatus.RUNNING, TaskStatus.COMPLETED)
    assert can_task(TaskStatus.RUNNING, TaskStatus.RETRYABLE)
    assert can_task(TaskStatus.RETRYABLE, TaskStatus.READY)
    assert can_task(TaskStatus.RETRYABLE, TaskStatus.FAILED)
    for s in TaskStatus:
        if not s.terminal:
            assert can_task(s, TaskStatus.CANCELLED), s
    assert not can_task(TaskStatus.PENDING, TaskStatus.RUNNING)  # must be READY first
    assert not can_task(TaskStatus.RUNNING, TaskStatus.FAILED)  # must go through RETRYABLE
    assert not can_task(TaskStatus.READY, TaskStatus.BLOCKED)  # deps were complete when it became READY
    assert not can_task(TaskStatus.COMPLETED, TaskStatus.READY)


def test_attempt_and_artifact():
    for s in (
        AttemptStatus.SUCCESS,
        AttemptStatus.FAILED,
        AttemptStatus.TIMEOUT,
        AttemptStatus.ABANDONED,
        AttemptStatus.CANCELLED,
    ):
        assert can_attempt(AttemptStatus.RUNNING, s)
    assert not can_attempt(AttemptStatus.SUCCESS, AttemptStatus.RUNNING)  # attempts are never reused
    assert can_artifact(ArtifactStatus.CANDIDATE, ArtifactStatus.ACCEPTED)
    assert can_artifact(ArtifactStatus.CANDIDATE, ArtifactStatus.SUPERSEDED)
    assert can_artifact(ArtifactStatus.ACCEPTED, ArtifactStatus.SUPERSEDED)
    assert not can_artifact(ArtifactStatus.SUPERSEDED, ArtifactStatus.ACCEPTED)
    assert not can_artifact(ArtifactStatus.REJECTED, ArtifactStatus.CANDIDATE)
