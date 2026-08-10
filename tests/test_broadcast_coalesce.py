"""Tests for WebSocket broadcast coalescing under MQTT flood."""
from __future__ import annotations

import unittest

from nornir_dashboard.store import DashboardStore
from nornir_dashboard.ws_broadcast import coalesce_broadcast_messages


class TestCoalesceBroadcastMessages(unittest.TestCase):
    def test_merges_consecutive_events(self) -> None:
        messages = [
            {
                "type": "event",
                "event": {"id": 1, "run_id": "R1"},
                "run": {"run_id": "R1", "error_count": 1},
            },
            {
                "type": "event",
                "event": {"id": 2, "run_id": "R1"},
                "run": {"run_id": "R1", "error_count": 2},
            },
            {
                "type": "event",
                "event": {"id": 3, "run_id": "R2"},
            },
        ]
        out = coalesce_broadcast_messages(messages)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["type"], "event_batch")
        self.assertEqual([e["id"] for e in out[0]["events"]], [1, 2, 3])
        self.assertEqual(out[0]["runs"][0]["error_count"], 2)
        self.assertEqual(len(out[0]["runs"]), 1)

    def test_run_deleted_flushes_pending_batch(self) -> None:
        messages = [
            {"type": "event", "event": {"id": 1, "run_id": "R1"}},
            {"type": "run_deleted", "run_id": "R1"},
            {"type": "event", "event": {"id": 2, "run_id": "R2"}},
        ]
        out = coalesce_broadcast_messages(messages)
        self.assertEqual(out[0]["type"], "event_batch")
        self.assertEqual(out[0]["events"][0]["id"], 1)
        self.assertEqual(out[1], {"type": "run_deleted", "run_id": "R1"})
        self.assertEqual(out[2]["type"], "event_batch")
        self.assertEqual(out[2]["events"][0]["id"], 2)


class TestPruneThrottledOnAdd(unittest.TestCase):
    def test_add_event_prunes_every_n_inserts(self) -> None:
        store = DashboardStore(":memory:", max_events_per_run=5)
        store._PRUNE_EVERY = 3
        store.ensure_run("R1")
        for i in range(9):
            store.add_event("R1", float(i), "log", "info", {"message": str(i)})
        count = sum(1 for _ in store.iter_events_for_export("R1"))
        self.assertEqual(count, 5)

    def test_prune_prefers_errors_over_newer_info(self) -> None:
        store = DashboardStore(":memory:", max_events_per_run=5)
        store.ensure_run("R1")
        store.add_event("R1", 1.0, "log", "error", {"message": "old-error"})
        for i in range(10):
            store.add_event("R1", 10.0 + i, "log", "info", {"message": f"info-{i}"})
        store.prune_events("R1")
        retained = list(store.iter_events_for_export("R1"))
        self.assertEqual(len(retained), 5)
        self.assertTrue(any(
            e["level"] == "error" and e["payload"].get("message") == "old-error"
            for e in retained
        ))


if __name__ == "__main__":
    unittest.main()
