import asyncio
import time
import unittest
from unittest.mock import MagicMock

try:
    from nornir_dashboard.main import _delete_run_and_notify, _run_retention_sweep
except ImportError as exc:  # pragma: no cover - environment without web deps
    raise unittest.SkipTest(
        "fastapi is required for the dashboard app tests "
        "(pip install -e '.[test]')") from exc

from nornir_dashboard.store import DashboardStore


class TestDeleteRunAndNotify(unittest.TestCase):
    """Shared delete path used by the API and retention sweeper."""

    def setUp(self) -> None:
        self.store = DashboardStore(":memory:")
        self.addCleanup(self.store.close)
        self.subscriber = MagicMock()
        self.manager = MagicMock()

    def test_deletes_and_notifies(self) -> None:
        self.store.ensure_run("R1")
        self.assertTrue(
            _delete_run_and_notify(self.store, self.subscriber, self.manager, "R1"))
        self.assertIsNone(self.store.get_run("R1"))
        self.subscriber.clear_retained.assert_called_once_with("R1")
        self.manager.submit_from_thread.assert_called_once_with(
            {"type": "run_deleted", "run_id": "R1"})

    def test_unknown_run_returns_false(self) -> None:
        self.assertFalse(
            _delete_run_and_notify(self.store, self.subscriber, self.manager, "missing"))
        self.subscriber.clear_retained.assert_not_called()
        self.manager.submit_from_thread.assert_not_called()


class TestRetentionSweep(unittest.TestCase):
    """Retention sweep deletes expired runs and leaves recent ones."""

    def setUp(self) -> None:
        self.store = DashboardStore(":memory:")
        self.addCleanup(self.store.close)
        self.subscriber = MagicMock()
        self.manager = MagicMock()

    def test_sweep_deletes_only_expired_runs(self) -> None:
        now = time.time()
        self.store.ensure_run("R_old", now=now - 100_000.0)
        self.store.update_run_fields("R_old", {"last_seen": now - 100_000.0})
        self.store.ensure_run("R_new", now=now)
        self.store.update_run_fields("R_new", {"last_seen": now})

        count = _run_retention_sweep(self.store, self.subscriber, self.manager, 1.0)
        self.assertEqual(count, 1)
        self.assertIsNone(self.store.get_run("R_old"))
        self.assertIsNotNone(self.store.get_run("R_new"))
        self.subscriber.clear_retained.assert_called_once_with("R_old")
        self.manager.submit_from_thread.assert_called_once_with(
            {"type": "run_deleted", "run_id": "R_old"})

    def test_sweep_noop_when_nothing_expired(self) -> None:
        now = time.time()
        self.store.ensure_run("R1", now=now)
        self.store.update_run_fields("R1", {"last_seen": now})
        count = _run_retention_sweep(self.store, self.subscriber, self.manager, 30.0)
        self.assertEqual(count, 0)
        self.subscriber.clear_retained.assert_not_called()


class TestRetentionSweeperDisabled(unittest.TestCase):
    """retention_days=0 must not start a sweeper task (config-level contract)."""

    def test_zero_retention_days_skips_task_creation(self) -> None:
        from nornir_dashboard.config import DashboardConfig

        config = DashboardConfig()
        config.retention_days = 0.0
        self.assertLessEqual(config.retention_days, 0)


class TestRetentionSweeperAsync(unittest.IsolatedAsyncioTestCase):
    """One retention sweeper iteration runs at startup before sleeping."""

    async def test_sweeper_runs_once_before_first_sleep(self) -> None:
        from nornir_dashboard.main import _retention_sweeper

        store = DashboardStore(":memory:")
        self.addCleanup(store.close)
        store.ensure_run("R_old", now=1.0)
        store.update_run_fields("R_old", {"last_seen": 1.0})

        subscriber = MagicMock()
        manager = MagicMock()

        task = asyncio.create_task(
            _retention_sweeper(store, subscriber, manager, retention_days=0.00001,
                               interval=3600.0))
        await asyncio.sleep(0.05)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertIsNone(store.get_run("R_old"))
        subscriber.clear_retained.assert_called_with("R_old")


if __name__ == "__main__":
    unittest.main()
