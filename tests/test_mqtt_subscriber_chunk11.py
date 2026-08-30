"""Subscriber-level fixes from the chunk 11 bug review."""
import json
import threading
import unittest
from unittest.mock import MagicMock

import paho.mqtt.client as mqtt

from nornir_dashboard.mqtt_subscriber import MqttSubscriber
from nornir_dashboard.store import DashboardStore


class SubscriberTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.store = DashboardStore(":memory:")
        self.addCleanup(self.store.close)
        self.broadcast = MagicMock()
        self.subscriber = MqttSubscriber(
            store=self.store,
            host="127.0.0.1",
            port=1883,
            keepalive=60,
            topic_root="nornir/run",
            broadcast=self.broadcast,
        )

    def publish(self, leaf: str, payload: dict, run_id: str = "R1") -> None:
        self.subscriber._handle_message(
            f"nornir/run/{run_id}/{leaf}", json.dumps(payload).encode("utf-8"))


class TestMixedDepthTypes(SubscriberTestCase):
    """C11-B001: a string depth beside an int depth must not lose the message."""

    def test_string_depth_alongside_int_depth_is_projected(self) -> None:
        self.publish("event", {
            "event": "iterate_progress",
            "track_id": "iterate:SectionNode",
            "label": "sections",
            "depth": "0",
            "current": 3,
            "total": 10,
            "section": 63,
        })
        # _update_progress uses a hardcoded int depth of 100 for labeled bars.
        self.publish("progress", {"label": "Assemble", "progress": 1, "total": 4})

        run = self.store.get_run("R1")
        assert run is not None
        self.assertEqual(run["current_section"], "63")
        # The shallowest track ("0") wins over the depth-100 labeled bar.
        self.assertEqual(run["progress_total"], 10)

    def test_unparsable_depth_falls_back_to_zero(self) -> None:
        self.publish("event", {
            "event": "iterate_progress",
            "track_id": "t1",
            "label": "t1",
            "depth": "deep",
            "current": 1,
            "total": 2,
        })
        tracks = self.store.get_progress_tracks("R1")
        self.assertEqual(tracks["t1"]["depth"], 0)

    def test_string_depth_is_stored_as_a_number(self) -> None:
        self.publish("event", {
            "event": "iterate_progress",
            "track_id": "t1", "label": "t1", "depth": "2",
            "current": 1, "total": 2,
        })
        tracks = self.store.get_progress_tracks("R1")
        self.assertEqual(tracks["t1"]["depth"], 2)

    def test_stage_fields_survive_a_bad_depth(self) -> None:
        """The pending current_stage write used to be lost to the sort TypeError."""
        self.publish("event", {
            "event": "iterate_progress",
            "track_id": "t1", "label": "t1", "depth": "0",
            "current": 1, "total": 2, "element": "TEM",
        })
        run = self.store.get_run("R1")
        assert run is not None
        self.assertEqual(run["current_element"], "TEM")
        self.assertTrue(self.broadcast.called)


class TestBadTimestamp(SubscriberTestCase):
    """C11-B003: a malformed ts must not discard the whole message."""

    def test_error_with_bad_ts_is_still_counted_and_stored(self) -> None:
        self.publish("log/error", {"ts": "not-a-number", "message": "boom"})

        run = self.store.get_run("R1")
        assert run is not None
        self.assertEqual(run["error_count"], 1)

        events = self.store.get_events("R1")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["payload"]["message"], "boom")
        self.assertGreater(events[0]["ts"], 0)
        self.assertTrue(self.broadcast.called)

    def test_bad_ts_falls_back_to_arrival_time(self) -> None:
        import time
        before = time.time()
        self.publish("log/info", {"ts": {"nested": "object"}, "message": "hi"})
        events = self.store.get_events("R1")
        self.assertGreaterEqual(events[0]["ts"], before)

    def test_valid_ts_is_preserved(self) -> None:
        self.publish("log/info", {"ts": 1234.5, "message": "hi"})
        events = self.store.get_events("R1")
        self.assertEqual(events[0]["ts"], 1234.5)

    def test_string_numeric_ts_is_accepted(self) -> None:
        self.publish("log/info", {"ts": "1234.5", "message": "hi"})
        events = self.store.get_events("R1")
        self.assertEqual(events[0]["ts"], 1234.5)


class TestStaleRevival(SubscriberTestCase):
    """C11-B002 end to end: reviving a stale run clears the old end time."""

    def test_revived_run_has_no_end_ts(self) -> None:
        self.publish("meta", {"pipeline": "Assemble", "status": "completed",
                              "end_ts": 500.0})
        self.store.update_run_fields("R1", {"status": "stale"})

        self.publish("log/info", {"message": "still going"})

        run = self.store.get_run("R1")
        assert run is not None
        self.assertEqual(run["status"], "running")
        self.assertIsNone(run["end_ts"])

    def test_terminal_payload_does_not_revive(self) -> None:
        self.publish("meta", {"pipeline": "Assemble", "status": "completed",
                              "end_ts": 500.0})
        self.store.update_run_fields("R1", {"status": "stale"})
        self.publish("meta", {"pipeline": "Assemble", "status": "completed"})
        run = self.store.get_run("R1")
        assert run is not None
        self.assertEqual(run["status"], "completed")


class TestStageFailed(SubscriberTestCase):
    """C11-B015: a failed stage must not keep rendering as running."""

    def test_stage_failed_sets_failed_status(self) -> None:
        self.publish("meta", {"pipeline": "Assemble", "status": "running"})
        self.publish("event", {"event": "stage_failed",
                               "module": "nornir_buildmanager.operations.tile",
                               "function": "BuildImagePyramid"})
        run = self.store.get_run("R1")
        assert run is not None
        self.assertEqual(run["status"], "failed")
        self.assertEqual(
            run["current_stage"],
            "nornir_buildmanager.operations.tile.BuildImagePyramid")

    def test_stage_failed_does_not_double_count_errors(self) -> None:
        """The same failure also arrives as a log/error line."""
        self.publish("event", {"event": "stage_failed", "module": "m",
                               "function": "f"})
        run = self.store.get_run("R1")
        assert run is not None
        self.assertIn(run["error_count"], (0, None))

    def test_stage_end_does_not_set_failed(self) -> None:
        self.publish("meta", {"pipeline": "Assemble", "status": "running"})
        self.publish("event", {"event": "stage_end", "module": "m", "function": "f"})
        run = self.store.get_run("R1")
        assert run is not None
        self.assertEqual(run["status"], "running")

    def test_stage_failed_records_end_ts_when_supplied(self) -> None:
        self.publish("event", {"event": "stage_failed", "module": "m",
                               "function": "f", "end_ts": 42.0})
        run = self.store.get_run("R1")
        assert run is not None
        self.assertEqual(run["end_ts"], 42.0)


class TestPerMessageTransaction(SubscriberTestCase):
    """C11-P001/P002/P003: one commit per message, no redundant reads/writes."""

    def _instrument(self):
        counters = {"commit": 0, "update": 0, "select_runs": 0}
        real_connection = self.store._connection
        subscriber_self = self

        class CountingConnection:
            def commit(self) -> None:
                counters["commit"] += 1
                real_connection.commit()

            def execute(self, sql, *args, **kwargs):
                stripped = " ".join(str(sql).split())
                if stripped.upper().startswith("UPDATE RUNS"):
                    counters["update"] += 1
                if "FROM runs" in stripped:
                    counters["select_runs"] += 1
                return real_connection.execute(sql, *args, **kwargs)

            def __getattr__(self, name):
                return getattr(real_connection, name)

        self.store._connection = CountingConnection()  # type: ignore[assignment]
        self.addCleanup(
            lambda: setattr(subscriber_self.store, "_connection", real_connection))
        return counters

    def test_info_log_costs_one_commit(self) -> None:
        self.publish("log/info", {"message": "warmup"})
        counters = self._instrument()
        self.publish("log/info", {"message": "hello"})
        self.assertEqual(counters["commit"], 1)

    def test_error_log_costs_one_commit(self) -> None:
        self.publish("log/info", {"message": "warmup"})
        counters = self._instrument()
        self.publish("log/error", {"message": "boom"})
        self.assertEqual(counters["commit"], 1)

    def test_iterate_progress_costs_one_commit(self) -> None:
        self.publish("log/info", {"message": "warmup"})
        counters = self._instrument()
        self.publish("event", {"event": "iterate_progress", "track_id": "t1",
                               "label": "t1", "depth": 0, "current": 1, "total": 9})
        self.assertEqual(counters["commit"], 1)

    def test_no_redundant_last_seen_update(self) -> None:
        """ensure_run's upsert already writes last_seen (C11-P002)."""
        self.publish("log/info", {"message": "warmup"})
        counters = self._instrument()
        self.publish("log/info", {"message": "hello"})
        self.assertEqual(counters["update"], 0)

    def test_info_log_does_not_read_the_full_run_row(self) -> None:
        """The stale check reads one column and no run summary is broadcast (C11-P003)."""
        self.publish("log/info", {"message": "warmup"})
        counters = self._instrument()
        self.publish("log/info", {"message": "hello"})
        self.assertEqual(counters["select_runs"], 1)

    def test_last_seen_still_advances(self) -> None:
        self.publish("log/info", {"message": "one"})
        run = self.store.get_run("R1")
        assert run is not None
        first = run["last_seen"]
        self.publish("log/info", {"message": "two"})
        run = self.store.get_run("R1")
        assert run is not None
        self.assertGreaterEqual(run["last_seen"], first)


class TestTrackMergeAtomicity(SubscriberTestCase):
    """C11-B014: the tracks read-modify-write happens under one lock hold."""

    def test_concurrent_clear_cannot_be_overwritten_mid_merge(self) -> None:
        self.publish("event", {"event": "iterate_progress", "track_id": "t1",
                               "label": "t1", "depth": 0, "current": 1, "total": 9})

        cleared = threading.Event()

        def clear_from_another_thread() -> None:
            self.store.clear_run_progress("R1")
            cleared.set()

        # The whole message is one transaction, so the clear can only land
        # before or after it, never between the read and the write.
        thread = threading.Thread(target=clear_from_another_thread)
        with self.store.transaction():
            thread.start()
            self.publish("event", {"event": "iterate_progress", "track_id": "t2",
                                   "label": "t2", "depth": 0, "current": 2,
                                   "total": 9})
        thread.join(timeout=5)
        self.assertTrue(cleared.is_set())

        tracks = self.store.get_progress_tracks("R1")
        self.assertEqual(tracks, {})


class TestRetainedClear(SubscriberTestCase):
    """C11-B005: retained clears use QoS 1 and report failures."""

    def test_clear_uses_qos_1_and_retain(self) -> None:
        self.subscriber._client = MagicMock()
        self.subscriber._client.publish.return_value = MagicMock(
            rc=mqtt.MQTT_ERR_SUCCESS)

        self.assertTrue(self.subscriber.clear_retained("R1"))

        self.subscriber._client.publish.assert_called_once_with(
            "nornir/run/R1/meta", payload=b"", qos=1, retain=True)

    def test_broker_rejection_is_reported(self) -> None:
        self.subscriber._client = MagicMock()
        self.subscriber._client.publish.return_value = MagicMock(
            rc=mqtt.MQTT_ERR_NO_CONN)

        with self.assertLogs("nornir_dashboard.mqtt_subscriber", level="WARNING"):
            self.assertFalse(self.subscriber.clear_retained("R1"))

    def test_publish_exception_is_reported(self) -> None:
        self.subscriber._client = MagicMock()
        self.subscriber._client.publish.side_effect = OSError("down")
        with self.assertLogs("nornir_dashboard.mqtt_subscriber", level="WARNING"):
            self.assertFalse(self.subscriber.clear_retained("R1"))

    def test_empty_run_id_is_a_noop(self) -> None:
        self.subscriber._client = MagicMock()
        self.assertFalse(self.subscriber.clear_retained(""))
        self.subscriber._client.publish.assert_not_called()


class TestStopJoinsConnectThread(SubscriberTestCase):
    """C11-B013: stop() must not leave a connect in flight."""

    def test_stop_joins_the_connect_thread(self) -> None:
        release = threading.Event()
        started = threading.Event()
        loop_started = threading.Event()

        client = MagicMock()

        def slow_connect(*_args, **_kwargs):
            started.set()
            release.wait(timeout=5)

        client.connect.side_effect = slow_connect
        client.loop_start.side_effect = lambda: loop_started.set()
        self.subscriber._client = client

        self.subscriber.start()
        self.assertTrue(started.wait(timeout=5))

        stopper = threading.Thread(target=self.subscriber.stop)
        stopper.start()
        release.set()
        stopper.join(timeout=5)
        self.assertFalse(stopper.is_alive())

        thread = self.subscriber._connect_thread
        assert thread is not None
        self.assertFalse(thread.is_alive())
        # stop() ran during connect(), so no network loop may be started.
        self.assertFalse(loop_started.is_set())
        client.disconnect.assert_called()

    def test_stop_is_idempotent(self) -> None:
        self.subscriber._client = MagicMock()
        self.subscriber.stop()
        self.subscriber.stop()


class TestOtherKindIngestion(SubscriberTestCase):
    """C11-B010 end to end: an unmapped leaf is stored and remains filterable."""

    def test_unmapped_leaf_is_visible_under_the_other_filter(self) -> None:
        self.publish("telemetry/gpu", {"message": "gpu 80%"})
        events = self.store.get_events("R1", types=["other"])
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["kind"], "other")


if __name__ == "__main__":
    unittest.main()
