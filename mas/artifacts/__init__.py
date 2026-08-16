"""Immutable, attempt-versioned artifacts (ADR-002). Publish / accept / supersede / reject — never mutate."""

from mas.artifacts.store import (
    accept,
    accepted_for_run,
    for_task,
    outputs_of_dependencies,
    outputs_of_task,
    publish,
    reject,
    supersede,
)

__all__ = [
    "accept",
    "accepted_for_run",
    "for_task",
    "outputs_of_dependencies",
    "outputs_of_task",
    "publish",
    "reject",
    "supersede",
]
