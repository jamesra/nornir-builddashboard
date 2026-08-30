"""FastAPI application fixes from the chunk 11 bug review."""
import asyncio
import contextlib
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

try:
    from fastapi.testclient import TestClient
except ImportError as exc:  # pragma: no cover - environment without web deps
    raise unittest.SkipTest(
        "fastapi/httpx are required for the dashboard app tests "
        "(pip install -e '.[test]')") from exc

from nornir_dashboard.config import DashboardConfig
from nornir_dashboard.main import (
    BROADCAST_QUEUE_MAX,
    ConnectionManager,
    _run_stale_sweep,
    create_app,
)
from nornir_dashboard.store import RUNS_LIMIT_MAX, DashboardStore, clamp_runs_limit
from nornir_dashboard.ws_broadcast import (
    MAX_COALESCED_EVENTS,
    coalesce_broadcast_messages,
)


def _config(tmpdir: str, **overrides) -> DashboardConfig:
    with patch.dict(os.environ, {}, clear=False):
        for key in list(os.environ):
            if key.startswith(("NORNIR_MQTT_", "NORNIR_DASHBOARD_")):
                del os.environ[key]
        config = DashboardConfig()
    config.database_path = os.path.join(tmpdir, "test.db")
    for key, value in overrides.items():
        setattr(config, key, value)
    return config


class AppTestCase(unittest.TestCase):
    """Builds a real app without starting MQTT or the background sweepers."""

    config_overrides: dict = {}

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.config = _config(self._tmp.name, **self.config_overrides)
        self.app = create_app(self.config)
        self.store: DashboardStore = self.app.state.store
        self.addCleanup(self.store.close)
        # Nothing here should touch a broker.
        self.app.state.subscriber._client = MagicMock()

    @contextlib.contextmanager
    def client(self):
        """A client used without lifespan, so no sweepers or MQTT threads start."""
        yield TestClient(self.app, raise_server_exceptions=False)

    def seed(self, count: int) -> None:
        for index in range(count):
            run_id = f"R{index:03d}"
            self.store.ensure_run(run_id)
            self.store.update_run_fields(run_id, {"pipeline": "Assemble"})


class TestNoImportSideEffects(unittest.TestCase):
    """C11-B004: importing the module must not create a database."""

    def test_import_does_not_build_the_app(self) -> None:
        import nornir_dashboard.main as main

        self.assertNotIn("app", main.__dict__)

    def test_import_in_a_clean_cwd_creates_no_db_file(self) -> None:
        import subprocess
        import sys

        with tempfile.TemporaryDirectory() as tmp:
            env = {k: v for k, v in os.environ.items()
                   if not k.startswith("NORNIR_DASHBOARD_")}
            # The package may only be importable from the repo root (not installed).
            package_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            env["PYTHONPATH"] = os.pathsep.join(
                [package_root, env.get("PYTHONPATH", "")]).rstrip(os.pathsep)
            subprocess.run(
                [sys.executable, "-c", "import nornir_dashboard.main"],
                cwd=tmp, env=env, check=True, capture_output=True)
            self.assertEqual(os.listdir(tmp), [])

    def test_module_level_app_attribute_still_works(self) -> None:
        """uvicorn nornir_dashboard.main:app must keep working, but lazily."""
        import nornir_dashboard.main as main

        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "lazy.db")
            with patch.dict(os.environ, {"NORNIR_DASHBOARD_DB": db}):
                try:
                    application = main.app
                    self.assertTrue(hasattr(application, "router"))
                    self.assertTrue(os.path.isfile(db))
                    application.state.store.close()
                finally:
                    main.__dict__.pop("app", None)

    def test_unknown_attribute_still_raises(self) -> None:
        import nornir_dashboard.main as main

        with self.assertRaises(AttributeError):
            main.definitely_not_here  # noqa: B018


class TestRunsLimitClamp(AppTestCase):
    """C11-B008: LIMIT -1 must not dump the whole runs table."""

    def test_clamp_helper(self) -> None:
        self.assertEqual(clamp_runs_limit(-1), 1)
        self.assertEqual(clamp_runs_limit(0), 1)
        self.assertEqual(clamp_runs_limit(5), 5)
        self.assertEqual(clamp_runs_limit(10 ** 9), RUNS_LIMIT_MAX)
        self.assertEqual(clamp_runs_limit("bogus"), 200)  # type: ignore[arg-type]

    def test_negative_limit_returns_one_run(self) -> None:
        self.seed(5)
        with self.client() as client:
            body = client.get("/api/runs?limit=-1").json()
        self.assertEqual(len(body["runs"]), 1)

    def test_positive_limit_is_honored(self) -> None:
        self.seed(5)
        with self.client() as client:
            body = client.get("/api/runs?limit=3").json()
        self.assertEqual(len(body["runs"]), 3)


class TestStaticAssetGuard(AppTestCase):
    """C11-B017: a missing index.html must not be a 500."""

    def test_index_is_served_when_present(self) -> None:
        with self.client() as client:
            self.assertEqual(client.get("/").status_code, 200)

    def test_missing_index_returns_503(self) -> None:
        with patch("nornir_dashboard.main.os.path.isfile", return_value=False):
            with self.client() as client:
                response = client.get("/")
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["error"], "static_assets_missing")


class TestAuth(AppTestCase):
    """C11-S001: an optional token gates the API and the WebSocket."""

    config_overrides = {"auth_token": "s3cret"}

    def test_api_requires_the_token(self) -> None:
        with self.client() as client:
            self.assertEqual(client.get("/api/runs").status_code, 401)

    def test_bearer_header_is_accepted(self) -> None:
        with self.client() as client:
            response = client.get(
                "/api/runs", headers={"Authorization": "Bearer s3cret"})
        self.assertEqual(response.status_code, 200)

    def test_custom_header_is_accepted(self) -> None:
        with self.client() as client:
            response = client.get(
                "/api/runs", headers={"X-Dashboard-Token": "s3cret"})
        self.assertEqual(response.status_code, 200)

    def test_query_token_is_accepted(self) -> None:
        with self.client() as client:
            self.assertEqual(
                client.get("/api/runs?token=s3cret").status_code, 200)

    def test_wrong_token_is_rejected(self) -> None:
        with self.client() as client:
            self.assertEqual(client.get("/api/runs?token=nope").status_code, 401)

    def test_delete_requires_the_token(self) -> None:
        self.seed(1)
        with self.client() as client:
            self.assertEqual(client.delete("/api/runs/R000").status_code, 401)
        self.assertIsNotNone(self.store.get_run("R000"))

    def test_events_and_export_require_the_token(self) -> None:
        with self.client() as client:
            self.assertEqual(client.get("/api/runs/R000/events").status_code, 401)
            self.assertEqual(
                client.get("/api/runs/R000/events/export").status_code, 401)

    def test_index_and_static_are_not_gated(self) -> None:
        """The page has to load before it can present a token."""
        with self.client() as client:
            self.assertEqual(client.get("/").status_code, 200)


class TestNoAuthByDefault(AppTestCase):
    """With no token configured the API stays open (loopback default bind)."""

    def test_api_is_open(self) -> None:
        with self.client() as client:
            self.assertEqual(client.get("/api/runs").status_code, 200)

    def test_non_loopback_bind_without_token_warns(self) -> None:
        config = _config(self._tmp.name, http_host="0.0.0.0")
        config.database_path = os.path.join(self._tmp.name, "warn.db")
        with self.assertLogs("nornir_dashboard.main", level="WARNING") as logs:
            app = create_app(config)
        app.state.store.close()
        self.assertTrue(any("NORNIR_DASHBOARD_TOKEN" in line for line in logs.output))


class TestDeleteDisabled(AppTestCase):
    """C11-S001: the destructive endpoint can be turned off."""

    config_overrides = {"allow_delete": False}

    def test_delete_is_refused(self) -> None:
        self.seed(1)
        with self.client() as client:
            response = client.delete("/api/runs/R000")
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json()["error"], "delete_disabled")
        self.assertIsNotNone(self.store.get_run("R000"))

    def test_reads_still_work(self) -> None:
        self.seed(1)
        with self.client() as client:
            self.assertEqual(client.get("/api/runs").status_code, 200)


class TestDeleteEnabled(AppTestCase):
    def test_delete_removes_the_run(self) -> None:
        self.seed(1)
        with self.client() as client:
            self.assertEqual(client.delete("/api/runs/R000").status_code, 200)
        self.assertIsNone(self.store.get_run("R000"))

    def test_delete_unknown_run_is_404(self) -> None:
        with self.client() as client:
            self.assertEqual(client.delete("/api/runs/ghost").status_code, 404)


class TestWebSocketCleanup(unittest.IsolatedAsyncioTestCase):
    """C11-B011: cancellation must still unregister the client."""

    async def test_cancelled_send_still_unregisters(self) -> None:
        manager = ConnectionManager()
        manager.bind_loop(asyncio.get_running_loop())

        class FakeSocket:
            def __init__(self) -> None:
                self.sent: list = []

            async def accept(self) -> None:
                return None

            async def send_json(self, message) -> None:
                self.sent.append(message)

        socket = FakeSocket()
        await manager.register(socket)  # type: ignore[arg-type]
        self.assertEqual(len(manager._clients), 1)
        manager.unregister(socket)  # type: ignore[arg-type]
        self.assertEqual(len(manager._clients), 0)

    async def test_stalled_client_is_dropped_without_blocking_others(self) -> None:
        manager = ConnectionManager()
        manager.bind_loop(asyncio.get_running_loop())

        class SlowSocket:
            async def accept(self) -> None:
                return None

            async def send_json(self, message) -> None:
                await asyncio.sleep(60)

        class FastSocket:
            def __init__(self) -> None:
                self.sent: list = []

            async def accept(self) -> None:
                return None

            async def send_json(self, message) -> None:
                self.sent.append(message)

        slow = SlowSocket()
        fast = FastSocket()
        await manager.register(slow)  # type: ignore[arg-type]
        await manager.register(fast)  # type: ignore[arg-type]

        with patch("nornir_dashboard.main.CLIENT_SEND_TIMEOUT", 0.05):
            await manager._send_to_clients({"type": "ping"})

        self.assertEqual(len(fast.sent), 1)
        self.assertNotIn(slow, manager._clients)
        self.assertIn(fast, manager._clients)

    async def test_failing_client_is_dropped(self) -> None:
        manager = ConnectionManager()
        manager.bind_loop(asyncio.get_running_loop())

        class BrokenSocket:
            async def accept(self) -> None:
                return None

            async def send_json(self, message) -> None:
                raise RuntimeError("closed")

        broken = BrokenSocket()
        await manager.register(broken)  # type: ignore[arg-type]
        await manager._send_to_clients({"type": "ping"})
        self.assertEqual(len(manager._clients), 0)


class TestBroadcastQueueBound(unittest.IsolatedAsyncioTestCase):
    """C11-P007: the queue and the coalesced batch are both bounded."""

    async def test_queue_has_a_maxsize(self) -> None:
        manager = ConnectionManager()
        self.assertEqual(manager._queue.maxsize, BROADCAST_QUEUE_MAX)

    async def test_overflow_sheds_oldest_and_logs(self) -> None:
        manager = ConnectionManager(queue_maxsize=3)
        manager.bind_loop(asyncio.get_running_loop())
        with self.assertLogs("nornir_dashboard.main", level="WARNING"):
            for index in range(10):
                manager._enqueue({"type": "event", "event": {"id": index}})

        self.assertEqual(manager._queue.qsize(), 3)
        remaining = [manager._queue.get_nowait()["event"]["id"] for _ in range(3)]
        self.assertEqual(remaining, [7, 8, 9])

    def test_coalesce_caps_batch_size(self) -> None:
        messages = [{"type": "event", "event": {"id": i}}
                    for i in range(MAX_COALESCED_EVENTS + 10)]
        frames = coalesce_broadcast_messages(messages)
        self.assertEqual(len(frames), 2)
        self.assertEqual(len(frames[0]["events"]), MAX_COALESCED_EVENTS)
        self.assertEqual(len(frames[1]["events"]), 10)

    def test_coalesce_below_cap_is_one_frame(self) -> None:
        messages = [{"type": "event", "event": {"id": i}} for i in range(5)]
        frames = coalesce_broadcast_messages(messages)
        self.assertEqual(len(frames), 1)


class TestSweepsRunOffTheEventLoop(unittest.IsolatedAsyncioTestCase):
    """C11-B006: sweeps must not run SQLite on the loop thread."""

    async def test_stale_sweep_runs_in_a_worker_thread(self) -> None:
        import threading

        store = DashboardStore(":memory:")
        self.addCleanup(store.close)
        store.ensure_run("R1", now=0.0)
        store.update_run_fields("R1", {"pipeline": "Assemble"})

        manager = ConnectionManager()
        manager.bind_loop(asyncio.get_running_loop())
        loop_thread = threading.current_thread()
        observed: list = []

        real_mark = store.mark_stale_runs

        def tracking_mark(*args, **kwargs):
            observed.append(threading.current_thread())
            return real_mark(*args, **kwargs)

        store.mark_stale_runs = tracking_mark  # type: ignore[method-assign]

        stale, deleted = await asyncio.to_thread(
            _run_stale_sweep, store, manager, 1.0, None)

        self.assertEqual((stale, deleted), (1, 0))
        self.assertEqual(len(observed), 1)
        self.assertIsNot(observed[0], loop_thread)

    async def test_stale_sweep_notifies_clients(self) -> None:
        store = DashboardStore(":memory:")
        self.addCleanup(store.close)
        store.ensure_run("R1", now=0.0)
        store.update_run_fields("R1", {"pipeline": "Assemble"})
        store.ensure_run("stub", now=0.0)

        manager = ConnectionManager()
        manager.bind_loop(asyncio.get_running_loop())
        subscriber = MagicMock()

        stale, deleted = await asyncio.to_thread(
            _run_stale_sweep, store, manager, 1.0, subscriber)
        await asyncio.sleep(0)

        self.assertEqual((stale, deleted), (1, 1))
        self.assertEqual(subscriber.clear_retained.call_count, 2)
        types = []
        while not manager._queue.empty():
            types.append(manager._queue.get_nowait()["type"])
        self.assertEqual(sorted(types), ["event", "run_deleted"])


if __name__ == "__main__":
    unittest.main()
