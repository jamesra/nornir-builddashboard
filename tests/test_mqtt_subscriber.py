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

    def test_iterate_progress_complete_keeps_track(self) -> None:
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
        # Nested complete keeps the last snapshot sticky until stage changes.
        self.assertIn("import_idoc:tiles", tracks)
        self.assertEqual(tracks["import_idoc:tiles"]["current"], 10)
        self.assertIn("import_idoc:sections", tracks)
        # Top-level still driven by shallowest largest track
        self.assertEqual(run["progress_total"], 5)
        self.assertEqual(run["progress_current"], 1)

    def test_stage_start_different_stage_clears_progress_tracks(self) -> None:
        self._publish("event", {
            "event": "stage_start",
            "module": "nornir_buildmanager.operations.channel",
            "function": "CreateBlobFilter",
        })
        self._publish("event", {
            "event": "iterate_progress",
            "track_id": "iterate:ChannelNode",
            "label": "ChannelNode - TEM",
            "depth": 1,
            "current": 1,
            "total": 1,
        })
        run = self.store.get_run("R1")
        self.assertTrue(run["progress_tracks"])

        self._publish("event", {
            "event": "stage_start",
            "module": "nornir_buildmanager.operations.tile",
            "function": "Assemble",
        })
        run = self.store.get_run("R1")
        self.assertEqual(
            run["current_stage"],
            "nornir_buildmanager.operations.tile.Assemble",
        )
        self.assertEqual(run["progress_tracks"], {})
        self.assertIsNone(run["progress_total"])

    def test_stage_start_same_stage_keeps_progress_tracks(self) -> None:
        self._publish("event", {
            "event": "stage_start",
            "module": "nornir_buildmanager.operations.channel",
            "function": "CreateBlobFilter",
        })
        self._publish("event", {
            "event": "iterate_progress",
            "track_id": "iterate:SectionNode",
            "label": "section_node - 0769",
            "depth": 0,
            "current": 618,
            "total": 1336,
            "section": 769,
        })
        self._publish("event", {
            "event": "iterate_progress",
            "track_id": "iterate:ChannelNode",
            "label": "ChannelNode - TEM",
            "depth": 1,
            "current": 1,
            "total": 1,
        })
        self._publish("event", {
            "event": "iterate_progress_complete",
            "track_id": "iterate:ChannelNode",
            "total": 1,
        })
        # Same CreateBlobFilter stage for the next section must not clear tracks.
        self._publish("event", {
            "event": "stage_start",
            "module": "nornir_buildmanager.operations.channel",
            "function": "CreateBlobFilter",
            "section": 770,
        })
        run = self.store.get_run("R1")
        tracks = run["progress_tracks"]
        self.assertIn("iterate:ChannelNode", tracks)
        self.assertIn("iterate:SectionNode", tracks)
        self.assertEqual(run["progress_total"], 1336)
        self.assertEqual(run["current_section"], "770")

    def test_warning_increments_counter(self) -> None:
        self._publish("log/warning", {"message": "missing file"})
        run = self.store.get_run("R1")
        self.assertEqual(run["warning_count"], 1)

    def test_terminal_meta_clears_progress_tracks(self) -> None:
        self._publish("meta", {"pipeline": "AdjustContrast", "status": "running"})
        self._publish("event", {
            "event": "iterate_progress",
            "track_id": "iterate:SectionNode",
            "label": "section_node - 14302",
            "depth": 0,
            "current": 2,
            "total": 4,
        })
        run = self.store.get_run("R1")
        self.assertEqual(run["progress_total"], 4)
        self.assertTrue(run["progress_tracks"])

        self._publish("meta", {"status": "completed", "end_ts": 123.0})
        run = self.store.get_run("R1")
        self.assertEqual(run["status"], "completed")
        self.assertEqual(run["progress_tracks"], {})
        self.assertIsNone(run["progress_current"])
        self.assertIsNone(run["progress_total"])
        self.assertIsNone(run["progress_fraction"])
        self.assertEqual(run["pipeline"], "AdjustContrast")

    def test_pipeline_name_change_clears_progress_tracks(self) -> None:
        self._publish("meta", {"pipeline": "Mosaic", "status": "running"})
        self._publish("event", {
            "event": "iterate_progress",
            "track_id": "iterate:SectionNode",
            "label": "section",
            "depth": 0,
            "current": 1,
            "total": 10,
        })
        self._publish("meta", {"pipeline": "Prune", "status": "running"})
        run = self.store.get_run("R1")
        self.assertEqual(run["pipeline"], "Prune")
        self.assertEqual(run["progress_tracks"], {})
        self.assertIsNone(run["progress_total"])

    def test_stale_meta_clears_progress_tracks(self) -> None:
        self._publish("meta", {"pipeline": "Assemble", "status": "running"})
        self._publish("event", {
            "event": "iterate_progress",
            "track_id": "t1",
            "label": "t1",
            "depth": 0,
            "current": 3,
            "total": 9,
        })
        self._publish("meta", {"status": "stale"})
        run = self.store.get_run("R1")
        self.assertEqual(run["progress_tracks"], {})
        self.assertIsNone(run["progress_fraction"])


class TestListRunsHidesUnnamed(unittest.TestCase):
    def setUp(self) -> None:
        self.store = DashboardStore(":memory:")
        self.addCleanup(self.store.close)

    def test_list_runs_excludes_null_pipeline(self) -> None:
        self.store.ensure_run("stub")
        self.store.ensure_run("named")
        self.store.update_run_fields("named", {"pipeline": "Prune", "volumepath": "/data"})
        runs = self.store.list_runs()
        ids = [r["run_id"] for r in runs]
        self.assertEqual(ids, ["named"])
        self.assertIsNotNone(self.store.get_run("stub"))

    def test_mark_stale_deletes_unnamed_and_clears_named_progress(self) -> None:
        now = 1_000_000.0
        self.store.ensure_run("stub", now=now - 1000)
        self.store.update_run_fields("stub", {"last_seen": now - 1000})
        self.store.ensure_run("named", now=now - 1000)
        self.store.update_run_fields("named", {
            "pipeline": "Mosaic",
            "last_seen": now - 1000,
            "progress_tracks": {"t": {"label": "t", "current": 1, "total": 2}},
            "progress_total": 2,
            "progress_current": 1,
            "progress_fraction": 0.5,
        })
        stale_ids, deleted_ids = self.store.mark_stale_runs(600.0, now=now)
        self.assertEqual(stale_ids, ["named"])
        self.assertEqual(deleted_ids, ["stub"])
        self.assertIsNone(self.store.get_run("stub"))
        named = self.store.get_run("named")
        self.assertEqual(named["status"], "stale")
        self.assertEqual(named["progress_tracks"], {})
        self.assertIsNone(named["progress_total"])


if __name__ == "__main__":
    unittest.main()
