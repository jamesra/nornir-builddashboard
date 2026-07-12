"""FastAPI application exposing the nornir build dashboard.

Wires together the SQLite store, the MQTT subscriber, a REST API for run/event
history, and a WebSocket that streams live updates to connected browsers.
"""
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
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
        subscriber.start()
        try:
            yield
        finally:
            subscriber.stop()
            broadcaster_task.cancel()
            store.close()

    app = FastAPI(title="Nornir Build Dashboard", lifespan=lifespan)
    app.state.store = store
    app.state.config = config

    @app.get("/api/runs")
    def list_runs(limit: int = 200) -> dict[str, Any]:
        """Return summaries for the most recently active runs."""
        return {"runs": store.list_runs(limit=limit)}

    @app.get("/api/runs/{run_id}")
    def get_run(run_id: str) -> dict[str, Any]:
        """Return a single run summary."""
        return {"run": store.get_run(run_id)}

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
