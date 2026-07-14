"""Unit tests for MQTT subscriber state projection."""
import json
import unittest
from unittest.mock import MagicMock

from nornir_dashboard.mqtt_subscriber import MqttSubscriber
from nornir_dashboard.store import DashboardStore


class TestMqttSubscriberProjection(unittest.TestCase):
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

    def _publish(self, leaf: str, payload: dict) -> None:
        topic = f"nornir/run/R1/{leaf}"
        self.subscriber._handle_message(topic, json.dumps(payload).encode("utf-8"))

    def test_meta_projects_compute(self) -> None:
        self._publish("meta", {
            "pipeline": "Assemble",
            "volumepath": "/data",
            "compute": "cupy",
            "status": "running",
            "host": "box",
        })
        run = self.store.get_run("R1")
        self.assertIsNotNone(run)
        self.assertEqual(run["compute"], "cupy")
        self.assertEqual(run["pipeline"], "Assemble")
        self.assertEqual(run["host"], "box")

    def test_nested_iterate_progress_tracks(self) -> None:
        self._publish("event", {
            "event": "iterate_progress",
            "track_id": "iterate:SectionNode",
            "label": "section_node - 0063",
            "depth": 0,
            "current": 701,
            "total": 756,
            "section": 63,
        })
        self._publish("event", {
            "event": "iterate_progress",
            "track_id": "iterate:ChannelNode",
            "label": "ChannelNode - TEM",
            "depth": 1,
            "current": 1,
            "total": 1,
            "element": "TEM",
        })
        run = self.store.get_run("R1")
        tracks = run["progress_tracks"]
        self.assertIn("iterate:SectionNode", tracks)
        self.assertIn("iterate:ChannelNode", tracks)
        self.assertEqual(tracks["iterate:SectionNode"]["depth"], 0)
        self.assertEqual(tracks["iterate:ChannelNode"]["label"], "ChannelNode - TEM")
        # Top-level uses shallowest largest track
        self.assertEqual(run["progress_total"], 756)
        self.assertEqual(run["current_section"], "63")

    def test_warning_increments_counter(self) -> None:
        self._publish("log/warning", {"message": "missing file"})
        run = self.store.get_run("R1")
        self.assertEqual(run["warning_count"], 1)


if __name__ == "__main__":
    unittest.main()
