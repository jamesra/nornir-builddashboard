"""Tests for event pagination, search, type filters, and export streaming.

Manual UI checklist (log pane / Download logs):
- Uncheck info, events, and status; leave only errors → pane shows only error lines.
- With errors-only filter, Download logs transcript contains only error lines.
- Re-check info → info lines reappear after reload; WebSocket inserts respect checkboxes.
"""
import unittest

from nornir_dashboard.store import (
    EVENTS_LIMIT_MAX,
    DashboardStore,
    clamp_events_limit,
    parse_types_param,
)


class TestClampAndParse(unittest.TestCase):
    def test_clamp_events_limit(self) -> None:
        self.assertEqual(clamp_events_limit(0), 1)
        self.assertEqual(clamp_events_limit(-5), 1)
        self.assertEqual(clamp_events_limit(100), 100)
        self.assertEqual(clamp_events_limit(EVENTS_LIMIT_MAX + 1), EVENTS_LIMIT_MAX)

    def test_parse_types_param(self) -> None:
        self.assertIsNone(parse_types_param(None))
        self.assertIsNone(parse_types_param(""))
        self.assertEqual(parse_types_param("error,warning"), ["error", "warning"])
        self.assertEqual(parse_types_param("bogus"), [])
        self.assertEqual(parse_types_param(["Event", " status "]), ["event", "status"])


class TestEventsQuery(unittest.TestCase):
    def setUp(self) -> None:
        self.store = DashboardStore(":memory:", max_events_per_run=0)
        self.addCleanup(self.store.close)
        self.store.ensure_run("R1")
        # Mixed kinds for filter/search coverage.
        self.ids = {
            "info": self.store.add_event(
                "R1", 1.0, "log", "info", {"message": "hello alpha"}
            ),
            "error": self.store.add_event(
                "R1", 2.0, "log", "error", {"message": "fail beta"}
            ),
            "event": self.store.add_event(
                "R1", 3.0, "event", None, {"event": "StageStart", "function": "Align"}
            ),
            "status": self.store.add_event(
                "R1", 4.0, "status", None, {"message": "running gamma"}
            ),
            "warn": self.store.add_event(
                "R1", 5.0, "log", "warning", {"message": "caution alpha"}
            ),
            "debug": self.store.add_event(
                "R1", 6.0, "log", "debug", {"message": "trace delta"}
            ),
        }

    def test_newest_page_default(self) -> None:
        page = self.store.get_events("R1", limit=3)
        self.assertEqual(len(page), 3)
        self.assertEqual([e["id"] for e in page], [
            self.ids["status"], self.ids["warn"], self.ids["debug"],
        ])

    def test_before_id_loads_older(self) -> None:
        newest = self.store.get_events("R1", limit=2)
        before = newest[0]["id"]
        older = self.store.get_events("R1", before_id=before, limit=2)
        self.assertEqual([e["id"] for e in older], [
            self.ids["event"], self.ids["status"],
        ])

    def test_after_id_loads_newer(self) -> None:
        newer = self.store.get_events("R1", after_id=self.ids["event"], limit=10)
        self.assertEqual([e["id"] for e in newer], [
            self.ids["status"], self.ids["warn"], self.ids["debug"],
        ])

    def test_q_filters_payload_substring(self) -> None:
        hits = self.store.get_events("R1", q="alpha", limit=50)
        self.assertEqual({e["id"] for e in hits}, {self.ids["info"], self.ids["warn"]})

    def test_types_filter(self) -> None:
        hits = self.store.get_events("R1", types=["error", "event"], limit=50)
        self.assertEqual({e["id"] for e in hits}, {self.ids["error"], self.ids["event"]})

    def test_types_error_only(self) -> None:
        """types=error returns only kind=log with level=error (not events/status/info)."""
        hits = self.store.get_events("R1", types=["error"], limit=50)
        self.assertEqual([e["id"] for e in hits], [self.ids["error"]])
        self.assertEqual(hits[0]["kind"], "log")
        self.assertEqual(hits[0]["level"], "error")

    def test_export_types_error_only(self) -> None:
        exported = list(self.store.iter_events_for_export("R1", types=["error"]))
        self.assertEqual([e["id"] for e in exported], [self.ids["error"]])
        self.assertTrue(all(e["kind"] == "log" and e["level"] == "error" for e in exported))

    def test_q_and_types_combine(self) -> None:
        hits = self.store.get_events(
            "R1", q="alpha", types=["warning"], limit=50
        )
        self.assertEqual([e["id"] for e in hits], [self.ids["warn"]])

    def test_empty_types_match_nothing(self) -> None:
        hits = self.store.get_events("R1", types=[], limit=50)
        self.assertEqual(hits, [])

    def test_export_streams_oldest_first(self) -> None:
        exported = list(self.store.iter_events_for_export("R1", q="alpha"))
        self.assertEqual([e["id"] for e in exported], [
            self.ids["info"], self.ids["warn"],
        ])

    def test_prune_disabled_when_max_zero(self) -> None:
        for i in range(20):
            self.store.add_event("R1", float(i), "log", "info", {"message": str(i)})
        self.store.prune_events("R1")
        all_events = self.store.get_events("R1", after_id=0, limit=5000)
        # after_id=0 with newest_or_older True returns newest page only;
        # use export iterator for full count.
        count = sum(1 for _ in self.store.iter_events_for_export("R1"))
        self.assertGreaterEqual(count, 26)


class TestMaxEventsDefault(unittest.TestCase):
    def test_config_default_is_unlimited(self) -> None:
        import os
        from unittest.mock import patch

        from nornir_dashboard.config import DashboardConfig

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("NORNIR_DASHBOARD_MAX_EVENTS", None)
            config = DashboardConfig()
            self.assertEqual(config.max_events_per_run, 0)


if __name__ == "__main__":
    unittest.main()
