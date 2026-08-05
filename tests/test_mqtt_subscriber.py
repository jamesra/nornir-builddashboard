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
        self.assertEqual(run["current_element"], "TEM")

    def test_iterate_progress_projects_stos_path(self) -> None:
        self._publish("event", {
            "event": "iterate_progress",
            "track_id": "stos_refine:files",
            "label": "StosGridRefine → Grid16",
            "depth": 0,
            "current": 0,
            "total": 12,
            "section": 1333,
            "element": "1334-1333_ctrl-TEM_Leveled_map-TEM_Leveled.stos",
            "path": "/storage4/RC2/TEM/Grid16/1334-1333_ctrl-TEM_Leveled_map-TEM_Leveled.stos",
        })
        run = self.store.get_run("R1")
        self.assertEqual(run["current_section"], "1333")
        self.assertEqual(
            run["current_element"],
            "1334-1333_ctrl-TEM_Leveled_map-TEM_Leveled.stos",
        )
        self.assertEqual(
            run["current_path"],
            "/storage4/RC2/TEM/Grid16/1334-1333_ctrl-TEM_Leveled_map-TEM_Leveled.stos",
        )

    def test_iterate_progress_complete_removes_track(self) -> None:
        self._publish("event", {
            "event": "iterate_progress",
            "track_id": "import_idoc:sections",
            "label": "ImportIDoc",
            "depth": 0,
            "current": 1,
            "total": 5,
            "section": 1,
        })
        self._publish("event", {
            "event": "iterate_progress",
            "track_id": "import_idoc:tiles",
            "label": "Import tiles capture.idoc",
            "depth": 1,
            "current": 10,
            "total": 10,
        })
        self._publish("event", {
            "event": "iterate_progress_complete",
            "track_id": "import_idoc:tiles",
            "total": 10,
        })
        run = self.store.get_run("R1")
        tracks = run["progress_tracks"]
        self.assertNotIn("import_idoc:tiles", tracks)
        self.assertIn("import_idoc:sections", tracks)
        # Top-level falls back to remaining shallowest track
        self.assertEqual(run["progress_total"], 5)
        self.assertEqual(run["progress_current"], 1)

    def test_warning_increments_counter(self) -> None:
        self._publish("log/warning", {"message": "missing file"})
        run = self.store.get_run("R1")
        self.assertEqual(run["warning_count"], 1)


if __name__ == "__main__":
    unittest.main()
