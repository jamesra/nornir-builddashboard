"""FastAPI application exposing the nornir build dashboard.

Wires together the SQLite store, the MQTT subscriber, a REST API for run/event
history, and a WebSocket that streams live updates to connected browsers.
"""
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import os

from nornir_dashboard.config import DashboardConfig, load_config
from nornir_dashboard.mqtt_subscriber import MqttSubscriber
from nornir_dashboard.store import DashboardStore

logger = logging.getLogger(__name__)

_STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


class ConnectionManager:
    """Tracks connected WebSocket clients and fans out live updates to them.

    The MQTT subscriber runs on a paho network thread, so updates are marshalled
    onto the asyncio event loop via a thread-safe queue before delivery.
    """

    _loop: asyncio.AbstractEventLoop | None
    _queue: "asyncio.Queue[dict[str, Any]]"
    _clients: set[WebSocket]

    def __init__(self) -> None:
        self._loop = None
        self._queue = asyncio.Queue()
        self._clients = set()

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Record the event loop used to schedule cross-thread broadcasts."""
        self._loop = loop

    def submit_from_thread(self, message: dict[str, Any]) -> None:
        """Thread-safe entry point used by the MQTT subscriber to queue a message."""
        if self._loop is None:
            return
        self._loop.call_soon_threadsafe(self._queue.put_nowait, message)

    async def register(self, websocket: WebSocket) -> None:
        """Accept and track a new WebSocket client."""
        await websocket.accept()
        self._clients.add(websocket)

    def unregister(self, websocket: WebSocket) -> None:
        """Stop tracking a disconnected WebSocket client."""
        self._clients.discard(websocket)

    async def broadcaster(self) -> None:
        """Background task that delivers queued messages to all clients."""
        while True:
            message = await self._queue.get()
            stale: list[WebSocket] = []
            for client in list(self._clients):
                try:
                    await client.send_json(message)
                except Exception:
                    stale.append(client)
            for client in stale:
                self._clients.discard(client)


def _delete_run_and_notify(store: DashboardStore, subscriber: MqttSubscriber,
                           manager: ConnectionManager, run_id: str) -> bool:
    """Delete a run from the store, clear retained MQTT meta, and notify browsers."""
    if not store.delete_run(run_id):
        return False
    subscriber.clear_retained(run_id)
    manager.submit_from_thread({"type": "run_deleted", "run_id": run_id})
    return True


def _run_retention_sweep(store: DashboardStore, subscriber: MqttSubscriber,
                         manager: ConnectionManager, retention_days: float) -> int:
    """Delete runs older than *retention_days* and notify connected clients."""
    if retention_days <= 0:
        return 0
    expired = store.list_expired_run_ids(retention_days)
    deleted = 0
    for run_id in expired:
        if _delete_run_and_notify(store, subscriber, manager, run_id):
            deleted += 1
    return deleted


async def _retention_sweeper(store: DashboardStore, subscriber: MqttSubscriber,
                             manager: ConnectionManager, retention_days: float,
                             interval: float) -> None:
    """Background task that periodically deletes expired runs."""
    while True:
        try:
            count = _run_retention_sweep(store, subscriber, manager, retention_days)
            if count:
                logger.info("Retention sweep deleted %s run(s)", count)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Retention sweep failed: %s", exc)
        await asyncio.sleep(interval)


async def _stale_sweeper(store: DashboardStore, manager: ConnectionManager,
                         stale_after: float, interval: float) -> None:
    """Background task that marks quiet running builds as stale."""
    while True:
        try:
            if stale_after > 0:
                stale_ids = store.mark_stale_runs(stale_after)
                for run_id in stale_ids:
                    run = store.get_run(run_id)
                    if run is not None:
                        manager.submit_from_thread({
                            "type": "event",
                            "event": {
                                "id": 0,
                                "run_id": run_id,
                                "ts": run.get("last_seen") or 0,
                                "kind": "meta",
                                "level": None,
                                "payload": {"status": "stale"},
                            },
                            "run": run,
                        })
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Stale sweep failed: %s", exc)
        await asyncio.sleep(interval)


def create_app(config: DashboardConfig | None = None) -> FastAPI:
    """Build and configure the dashboard FastAPI application."""
    config = config or load_config()
    store = DashboardStore(config.database_path, config.max_events_per_run)
    manager = ConnectionManager()
    subscriber = MqttSubscriber(
        store=store,
        host=config.mqtt_host,
        port=config.mqtt_port,
        keepalive=config.mqtt_keepalive,
        topic_root=config.topic_root,
        broadcast=manager.submit_from_thread,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        manager.bind_loop(asyncio.get_running_loop())
        broadcaster_task = asyncio.create_task(manager.broadcaster())
        retention_task = None
        stale_task = None
        if config.retention_days > 0:
            retention_task = asyncio.create_task(
                _retention_sweeper(
                    store, subscriber, manager,
                    retention_days=config.retention_days,
                    interval=config.retention_sweep_interval,
                )
            )
        if config.stale_after_seconds > 0:
            stale_task = asyncio.create_task(
                _stale_sweeper(
                    store, manager,
                    stale_after=config.stale_after_seconds,
                    interval=config.stale_sweep_interval,
                )
            )
        subscriber.start()
        try:
            yield
        finally:
            subscriber.stop()
            broadcaster_task.cancel()
            if retention_task is not None:
                retention_task.cancel()
            if stale_task is not None:
                stale_task.cancel()
            store.close()

    app = FastAPI(title="Nornir Build Dashboard", lifespan=lifespan)
    app.state.store = store
    app.state.config = config
    app.state.subscriber = subscriber
    app.state.manager = manager

    @app.get("/api/runs")
    def list_runs(limit: int = 200) -> dict[str, Any]:
        """Return summaries: active runs first (by activity, then start), then inactive by start."""
        return {"runs": store.list_runs(limit=limit)}

    @app.get("/api/runs/{run_id}")
    def get_run(run_id: str) -> dict[str, Any]:
        """Return a single run summary."""
        return {"run": store.get_run(run_id)}

    @app.delete("/api/runs/{run_id}")
    def delete_run(run_id: str) -> JSONResponse:
        """Delete a run and notify live clients."""
        deleted = _delete_run_and_notify(store, subscriber, manager, run_id)
        if not deleted:
            return JSONResponse({"ok": False, "error": "not_found"}, status_code=404)
        return JSONResponse({"ok": True, "run_id": run_id})

    @app.get("/api/runs/{run_id}/events")
    def get_events(run_id: str, after_id: int = 0, limit: int = 2000) -> dict[str, Any]:
        """Return events for a run with id greater than ``after_id``."""
        return {"events": store.get_events(run_id, after_id=after_id, limit=limit)}

    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await manager.register(websocket)
        try:
            while True:
                # We do not expect inbound messages; this keeps the socket open
                # and detects client disconnects.
                await websocket.receive_text()
        except WebSocketDisconnect:
            manager.unregister(websocket)
        except Exception:
            manager.unregister(websocket)

    @app.get("/")
    def index() -> FileResponse:
        """Serve the dashboard single-page app."""
        return FileResponse(os.path.join(_STATIC_DIR, "index.html"))

    if os.path.isdir(_STATIC_DIR):
        app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")

    return app


app = create_app()


def run() -> None:
    """Console-script entry point: serve the dashboard with uvicorn."""
    import uvicorn

    config = load_config()
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host=config.http_host, port=config.http_port)


if __name__ == "__main__":
    run()
