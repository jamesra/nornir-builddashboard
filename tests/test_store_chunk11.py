"""Store-level fixes from the chunk 11 bug review."""
import logging
import sqlite3
import unittest

from nornir_dashboard.store import CLEAR, DashboardStore, parse_types_param


class StoreTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.store = DashboardStore(":memory:")
        self.addCleanup(self.store.close)


class TestClearSentinel(StoreTestCase):
    """C11-B002: a column must be settable back to NULL."""

    def test_none_leaves_column_untouched(self) -> None:
        self.store.ensure_run("R1")
        self.store.update_run_fields("R1", {"end_ts": 123.0})
        self.store.update_run_fields("R1", {"end_ts": None})
        run = self.store.get_run("R1")
        assert run is not None
        self.assertEqual(run["end_ts"], 123.0)

    def test_clear_writes_null(self) -> None:
        self.store.ensure_run("R1")
        self.store.update_run_fields("R1", {"end_ts": 123.0, "status": "completed"})
        self.store.update_run_fields("R1", {"status": "running", "end_ts": CLEAR})
        run = self.store.get_run("R1")
        assert run is not None
        self.assertIsNone(run["end_ts"])
        self.assertEqual(run["status"], "running")

    def test_clear_only_field_still_updates(self) -> None:
        self.store.ensure_run("R1")
        self.store.update_run_fields("R1", {"end_ts": 5.0})
        self.assertTrue(self.store.update_run_fields("R1", {"end_ts": CLEAR}))
        run = self.store.get_run("R1")
        assert run is not None
        self.assertIsNone(run["end_ts"])


class TestUpdateRowcount(StoreTestCase):
    """C11-B012: an update against a deleted run must be reported, not silent."""

    def test_update_unknown_run_returns_false_and_logs(self) -> None:
        with self.assertLogs("nornir_dashboard.store", level=logging.WARNING) as logs:
            result = self.store.update_run_fields("ghost", {"status": "running"})
        self.assertFalse(result)
        self.assertTrue(any("ghost" in line for line in logs.output))

    def test_update_known_run_returns_true(self) -> None:
        self.store.ensure_run("R1")
        self.assertTrue(self.store.update_run_fields("R1", {"status": "running"}))

    def test_no_recognized_fields_is_not_a_failure(self) -> None:
        self.store.ensure_run("R1")
        self.assertTrue(self.store.update_run_fields("R1", {"bogus": 1}))

    def test_increment_counter_reports_unknown_run(self) -> None:
        with self.assertLogs("nornir_dashboard.store", level=logging.WARNING):
            self.assertFalse(self.store.increment_counter("ghost", "error_count"))
        self.store.ensure_run("R1")
        self.assertTrue(self.store.increment_counter("R1", "error_count"))


class TestParseTypesFallback(unittest.TestCase):
    """C11-B009: unknown filter keys fall back to all types, never to nothing."""

    def test_all_unknown_keys_fall_back_to_all(self) -> None:
        self.assertIsNone(parse_types_param("bogus"))
        self.assertIsNone(parse_types_param("bogus,alsobogus"))
        self.assertIsNone(parse_types_param(["nope"]))

    def test_empty_and_none_still_mean_all(self) -> None:
        self.assertIsNone(parse_types_param(""))
        self.assertIsNone(parse_types_param(None))

    def test_known_keys_survive_and_unknown_are_dropped(self) -> None:
        self.assertEqual(parse_types_param("error,bogus"), ["error"])
        self.assertEqual(parse_types_param("Error, WARNING"), ["error", "warning"])


class TestOtherKindVisibility(StoreTestCase):
    """C11-B010: rows stored with an unmapped kind must be reachable."""

    def setUp(self) -> None:
        super().setUp()
        self.store.ensure_run("R1")
        self.store.add_event("R1", 1.0, "log", "error", {"message": "boom"})
        self.store.add_event("R1", 2.0, "other", None, {"message": "gpu telemetry"})
        self.store.add_event("R1", 3.0, "status", None, {"status": "running"})

    def test_other_is_a_recognized_filter_key(self) -> None:
        self.assertEqual(parse_types_param("other"), ["other"])

    def test_other_filter_returns_only_unmapped_kinds(self) -> None:
        events = self.store.get_events("R1", types=["other"])
        self.assertEqual([e["kind"] for e in events], ["other"])

    def test_other_rows_are_included_with_no_filter(self) -> None:
        kinds = {e["kind"] for e in self.store.get_events("R1")}
        self.assertIn("other", kinds)

    def test_other_rows_are_excluded_from_known_filters(self) -> None:
        events = self.store.get_events("R1", types=["error", "status"])
        self.assertEqual([e["kind"] for e in events], ["log", "status"])

    def test_other_rows_reachable_via_export(self) -> None:
        exported = list(self.store.iter_events_for_export("R1", types=["other"]))
        self.assertEqual(len(exported), 1)


class TestPruneCounterCleanup(unittest.TestCase):
    """C11-D001: the per-run prune counter must not outlive the run."""

    def setUp(self) -> None:
        self.store = DashboardStore(":memory:", max_events_per_run=10)
        self.addCleanup(self.store.close)

    def test_delete_run_drops_counter(self) -> None:
        self.store.ensure_run("R1")
        self.store.add_event("R1", 1.0, "log", "info", {})
        self.assertIn("R1", self.store._events_since_prune)
        self.store.delete_run("R1")
        self.assertNotIn("R1", self.store._events_since_prune)

    def test_stale_stub_delete_drops_counter(self) -> None:
        self.store.ensure_run("stub", now=0.0)
        self.store.add_event("stub", 1.0, "log", "info", {})
        self.assertIn("stub", self.store._events_since_prune)
        _stale, deleted = self.store.mark_stale_runs(60.0, now=10_000.0)
        self.assertEqual(deleted, ["stub"])
        self.assertNotIn("stub", self.store._events_since_prune)


class TestWriteThroughputPragmas(unittest.TestCase):
    """C11-P001: WAL and synchronous=NORMAL are applied to file-backed stores."""

    def test_file_store_uses_wal(self) -> None:
        import tempfile
        import os

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "d.db")
            store = DashboardStore(path)
            try:
                mode = store._connection.execute("PRAGMA journal_mode").fetchone()[0]
                sync = store._connection.execute("PRAGMA synchronous").fetchone()[0]
            finally:
                store.close()
        self.assertEqual(str(mode).lower(), "wal")
        self.assertEqual(int(sync), 1)


class TestTransactionBatching(StoreTestCase):
    """C11-P001/C11-B014: several writes can share one commit, atomically."""

    def _count_commits(self):
        """Count commits by proxying the connection (sqlite3.commit is read-only)."""
        commits = {"n": 0}
        real_connection = self.store._connection

        class CountingConnection:
            def commit(self) -> None:
                commits["n"] += 1
                real_connection.commit()

            def __getattr__(self, name):
                return getattr(real_connection, name)

        self.store._connection = CountingConnection()  # type: ignore[assignment]
        self.addCleanup(
            lambda: setattr(self.store, "_connection", real_connection))
        return commits

    def test_transaction_collapses_commits(self) -> None:
        self.store.ensure_run("R1")
        commits = self._count_commits()
        with self.store.transaction():
            self.store.update_run_fields("R1", {"status": "running"})
            self.store.increment_counter("R1", "error_count")
            self.store.add_event("R1", 1.0, "log", "error", {"message": "x"})
        self.assertEqual(commits["n"], 1)

    def test_without_transaction_each_write_commits(self) -> None:
        self.store.ensure_run("R1")
        commits = self._count_commits()
        self.store.update_run_fields("R1", {"status": "running"})
        self.store.increment_counter("R1", "error_count")
        self.assertEqual(commits["n"], 2)

    def test_transaction_rolls_back_on_error(self) -> None:
        self.store.ensure_run("R1")
        self.store.update_run_fields("R1", {"status": "running"})
        with self.assertRaises(RuntimeError):
            with self.store.transaction():
                self.store.update_run_fields("R1", {"status": "failed"})
                raise RuntimeError("boom")
        run = self.store.get_run("R1")
        assert run is not None
        self.assertEqual(run["status"], "running")

    def test_nested_transactions_commit_once_at_the_outermost_exit(self) -> None:
        self.store.ensure_run("R1")
        commits = self._count_commits()
        with self.store.transaction():
            with self.store.transaction():
                self.store.update_run_fields("R1", {"status": "running"})
            self.assertEqual(commits["n"], 0)
        self.assertEqual(commits["n"], 1)


class TestSweepIndexes(StoreTestCase):
    """C11-P008: both sweeps must be index-backed, not full table scans."""

    def _plan(self, sql: str, params: tuple) -> str:
        rows = self.store._connection.execute(
            f"EXPLAIN QUERY PLAN {sql}", params).fetchall()
        return " ".join(str(row[-1]) for row in rows)

    def test_activity_indexes_exist(self) -> None:
        names = {
            row[0] for row in self.store._connection.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='runs'")
        }
        self.assertIn("idx_runs_activity", names)
        self.assertIn("idx_runs_status_activity", names)

    def test_retention_scan_uses_an_index(self) -> None:
        plan = self._plan(
            "SELECT run_id FROM runs WHERE COALESCE(last_seen, first_seen, 0) < ?",
            (0.0,))
        self.assertIn("idx_runs_activity", plan)

    def test_stale_scan_uses_an_index(self) -> None:
        plan = self._plan(
            "SELECT run_id, pipeline FROM runs "
            "WHERE status='running' AND COALESCE(last_seen, first_seen, 0) < ?",
            (0.0,))
        self.assertIn("idx_runs_status_activity", plan)


class TestEventSearchStaysRunScoped(StoreTestCase):
    """C11-P006: the LIKE search must still be narrowed by the run_id index."""

    def test_search_plan_uses_the_events_run_index(self) -> None:
        self.store.ensure_run("R1")
        self.store.add_event("R1", 1.0, "log", "info", {"message": "needle"})
        rows = self.store._connection.execute(
            "EXPLAIN QUERY PLAN SELECT id FROM events "
            "WHERE run_id = ? AND lower(COALESCE(payload, '')) LIKE ? "
            "ORDER BY id DESC LIMIT ?",
            ("R1", "%needle%", 10),
        ).fetchall()
        plan = " ".join(str(row[-1]) for row in rows)
        self.assertIn("idx_events_run", plan)
        self.assertNotIn("SCAN events", plan)

    def test_search_matches_payload_text(self) -> None:
        self.store.ensure_run("R1")
        self.store.add_event("R1", 1.0, "log", "info", {"message": "NEEDLE here"})
        self.store.add_event("R1", 2.0, "log", "info", {"message": "hay"})
        found = self.store.get_events("R1", q="needle")
        self.assertEqual(len(found), 1)


if __name__ == "__main__":
    unittest.main()
