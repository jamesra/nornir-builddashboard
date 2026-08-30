"""Helpers for coalescing live dashboard WebSocket frames."""
from __future__ import annotations

from typing import Any

# Largest number of events packed into a single event_batch frame. Without a cap
# a flood produces one enormous frame that has to be serialized and delivered as
# a unit, which stalls every client at once.
MAX_COALESCED_EVENTS = 500


def coalesce_broadcast_messages(
    messages: list[dict[str, Any]],
    max_events: int = MAX_COALESCED_EVENTS,
) -> list[dict[str, Any]]:
    """Merge consecutive live ``event`` frames into ``event_batch`` frames.

    ``run_deleted`` and other types flush any pending event batch first so
    clients see deletions in order relative to surrounding telemetry. A batch is
    also flushed once it reaches *max_events*.
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
            if max_events > 0 and len(pending_events) >= max_events:
                flush_events()
            continue
        flush_events()
        out.append(message)
    flush_events()
    return out
