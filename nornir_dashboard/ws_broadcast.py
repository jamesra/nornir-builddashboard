"""Helpers for coalescing live dashboard WebSocket frames."""
from __future__ import annotations

from typing import Any


def coalesce_broadcast_messages(
    messages: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Merge consecutive live ``event`` frames into one ``event_batch``.

    ``run_deleted`` and other types flush any pending event batch first so
    clients see deletions in order relative to surrounding telemetry.
    """
    out: list[dict[str, Any]] = []
    pending_events: list[dict[str, Any]] = []
    pending_runs: dict[str, dict[str, Any]] = {}

    def flush_events() -> None:
        nonlocal pending_events, pending_runs
        if not pending_events and not pending_runs:
            return
        out.append({
            "type": "event_batch",
            "events": pending_events,
            "runs": list(pending_runs.values()),
        })
        pending_events = []
        pending_runs = {}

    for message in messages:
        if message.get("type") == "event":
            event = message.get("event")
            if isinstance(event, dict):
                pending_events.append(event)
            run = message.get("run")
            if isinstance(run, dict) and run.get("run_id"):
                pending_runs[str(run["run_id"])] = run
            continue
        flush_events()
        out.append(message)
    flush_events()
    return out
