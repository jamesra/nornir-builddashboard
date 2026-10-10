"""Pin the progress-fraction arithmetic shared by the MqttSubscriber progress paths.

``_merge_progress_track``, ``_refresh_top_level_progress`` and ``_update_progress``
each derive a completion fraction from publisher-supplied ``current``/``total``
values. The publisher is another process, so those values arrive as any JSON
type. The tests compare the stored fractions against a literal restatement of
the original per-site arithmetic.
"""
import json
import unittest
from typing import Any
from unittest.mock import MagicMock

from hypothesis import given, settings
from hypothesis import strategies as st

from nornir_dashboard import mqtt_subscriber
from nornir_dashboard.mqtt_subscriber import MqttSubscriber
from nornir_dashboard.store import DashboardStore

# JSON-representable values a publisher could put in current/total. Numeric
# strings and non-numeric strings exercise the float() failure paths; NaN and
# infinity are left out because SQLite REAL columns do not round-trip them.
_values = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-10 ** 6, max_value=10 ** 6),
    st.floats(min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False),
    st.sampled_from(["", "0", "3", "2.5", "abc", "-4"]),
)


def _old_fraction(numerator: Any, total: Any) -> float | None:
    """The arithmetic each site hand-rolled: falsy total or unparsable -> None."""
    if not total:
        return None
    try:
        return float(numerator) / float(total)
    except (TypeError, ZeroDivisionError, ValueError):
        return None


def _run_with_fresh_subscriber(leaf: str, payload: dict[str, Any]) -> dict[str, Any]:
    store = DashboardStore(":memory:")
    try:
        subscriber = MqttSubscriber(
            store=store, host="127.0.0.1", port=1883, keepalive=60,
            topic_root="nornir/run", broadcast=MagicMock(),
        )
        subscriber._handle_message(f"nornir/run/R1/{leaf}", json.dumps(payload).encode("utf-8"))
        run = store.get_run("R1")
        return run if run is not None else {}
    finally:
        store.close()


class TestProgressFractionRouting(unittest.TestCase):
    def test_iterate_progress_fraction_for_numeric_values(self) -> None:
        run = _run_with_fresh_subscriber("event", {
            "event": "iterate_progress", "track_id": "t", "depth": 0, "current": 3, "total": 4,
        })
        self.assertEqual(run["progress_tracks"]["t"]["fraction"], 0.75)
        self.assertEqual(run["progress_fraction"], 0.75)

    def test_iterate_progress_missing_current_uses_zero_for_top_level(self) -> None:
        # The track has no fraction (current is None) but the sidebar treats a
        # missing current as 0 when a total is known.
        run = _run_with_fresh_subscriber("event", {
            "event": "iterate_progress", "track_id": "t", "depth": 0, "total": 8,
        })
        self.assertIsNone(run["progress_tracks"]["t"]["fraction"])
        self.assertEqual(run["progress_fraction"], 0.0)

    def test_zero_total_yields_no_fraction(self) -> None:
        run = _run_with_fresh_subscriber("event", {
            "event": "iterate_progress", "track_id": "t", "depth": 0, "current": 1, "total": 0,
        })
        self.assertIsNone(run["progress_tracks"]["t"]["fraction"])
        self.assertIsNone(run["progress_fraction"])

    def test_string_zero_total_does_not_raise(self) -> None:
        # "0" is truthy, so the division runs and ZeroDivisionError must be absorbed.
        run = _run_with_fresh_subscriber("event", {
            "event": "iterate_progress", "track_id": "t", "depth": 0, "current": 1, "total": "0",
        })
        self.assertIsNone(run["progress_tracks"]["t"]["fraction"])
        self.assertIsNone(run["progress_fraction"])

    def test_progress_leaf_fraction(self) -> None:
        run = _run_with_fresh_subscriber("progress", {"progress": 1, "total": 4})
        self.assertEqual(run["progress_fraction"], 0.25)

    def test_progress_leaf_missing_progress_has_no_fraction(self) -> None:
        run = _run_with_fresh_subscriber("progress", {"total": 4})
        self.assertIsNone(run.get("progress_fraction"))

    def test_explicit_fraction_wins(self) -> None:
        run = _run_with_fresh_subscriber("progress", {"progress": 1, "total": 4, "fraction": 0.9})
        self.assertEqual(run["progress_fraction"], 0.9)

    @settings(max_examples=150, deadline=None)
    @given(current=_values, total=_values)
    def test_iterate_progress_matches_original_arithmetic(self, current: Any, total: Any) -> None:
        payload: dict[str, Any] = {"event": "iterate_progress", "track_id": "t", "depth": 0}
        if current is not None:
            payload["current"] = current
        if total is not None:
            payload["total"] = total
        run = _run_with_fresh_subscriber("event", payload)

        track_fraction = _old_fraction(current, total) if current is not None else None
        self.assertEqual(run["progress_tracks"]["t"]["fraction"], track_fraction)
        expected_top = track_fraction
        if expected_top is None:
            expected_top = _old_fraction(current or 0, total)
        self.assertEqual(run.get("progress_fraction"), expected_top)

    @settings(max_examples=150, deadline=None)
    @given(progress=_values, total=_values)
    def test_progress_leaf_matches_original_arithmetic(self, progress: Any, total: Any) -> None:
        payload: dict[str, Any] = {}
        if progress is not None:
            payload["progress"] = progress
        if total is not None:
            payload["total"] = total
        run = _run_with_fresh_subscriber("progress", payload)
        self.assertEqual(run.get("progress_fraction"), _old_fraction(progress, total))


class TestProgressFractionHelper(unittest.TestCase):
    @unittest.skipUnless(hasattr(mqtt_subscriber, "_progress_fraction"), "helper not extracted yet")
    @settings(max_examples=200, deadline=None)
    @given(numerator=_values, total=_values)
    def test_helper_matches_original_arithmetic(self, numerator: Any, total: Any) -> None:
        self.assertEqual(mqtt_subscriber._progress_fraction(numerator, total),
                         _old_fraction(numerator, total))


if __name__ == "__main__":
    unittest.main()
