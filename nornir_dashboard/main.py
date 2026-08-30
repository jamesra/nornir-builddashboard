"""FastAPI application exposing the nornir build dashboard.

Wires together the SQLite store, the MQTT subscriber, a REST API for run/event
history, and a WebSocket that streams live updates to connected browsers.
"""
import asyncio
import hmac
import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, Iterator

from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    Request,
    Response,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from nornir_dashboard.config import DashboardConfig, load_config
from nornir_dashboard.mqtt_subscriber import MqttSubscriber
from nornir_dashboard.store import (
    EVENTS_LIMIT_DEFAULT,
    EVENTS_LIMIT_MAX,
    DashboardStore,
    clamp_events_limit,
    clamp_runs_limit,
    parse_types_param,
)
from nornir_dashboard.ws_broadcast import (
    MAX_COALESCED_EVENTS,
    coalesce_broadcast_messages,
)

logger = logging.getLogger(__name__)

_STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
_INDEX_HTML = os.path.join(_STATIC_DIR, "index.html")

# Bound on queued broadcast messages. Beyond this the oldest frames are dropped:
# a browser that cannot keep up with a log flood is better served by a gap than
# by unbounded memory growth on the server.
BROADCAST_QUEUE_MAX = 10_000

# A client that cannot accept a frame within this many seconds is treated as
# stalled and dropped, so it cannot hold up delivery to everyone else.
CLIENT_SEND_TIMEOUT = 5.0

_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})


class ConnectionManager:
    """Tracks connected WebSocket clients and fans out live updates to them.

    The MQTT subscriber runs on a paho network thread, so updates are marshalled
    onto the asyncio event loop via a thread-safe queue before delivery.
    Under flood, queued ``event`` messages are drained and coalesced into a
    single ``event_batch`` WebSocket frame per wake-up.
    """

    _loop: asyncio.AbstractEventLoop | None
    _queue: "asyncio.Queue[dict[str, Any]]"
    _clients: set[WebSocket]

    _dropped: int

    def __init__(self, queue_maxsize: int = BROADCAST_QUEUE_MAX) -> None:
        self._loop = None
        self._queue = asyncio.Queue(maxsize=queue_maxsize)
        self._clients = set()
        self._dropped = 0

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Record the event loop used to schedule cross-thread broadcasts."""
        self._loop = loop

    def submit_from_thread(self, message: dict[str, Any]) -> None:
        """Thread-safe entry point used by the MQTT subscriber to queue a message."""
        if self._loop is None:
            return
        self._loop.call_soon_threadsafe(self._enqueue, message)

    def _enqueue(self, message: dict[str, Any]) -> None:
        """Queue a message on the loop thread, shedding the oldest frame when full."""
        try:
            self._queue.put_nowait(message)
            return
        except asyncio.QueueFull:
            pass
        try:
            self._queue.get_nowait()
        except asyncio.QueueEmpty:  # pragma: no cover - drained concurrently
            pass
        self._dropped += 1
        if self._dropped % 1000 == 1:
            logger.warning(
                "Broadcast queue full (%s frames); dropped %s message(s) so far",
                self._queue.maxsize, self._dropped)
        try:
            self._queue.put_nowait(message)
        except asyncio.QueueFull:  # pragma: no cover - refilled concurrently
            pass

    async def register(self, websocket: WebSocket) -> None:
        """Accept and track a new WebSocket client."""
        await websocket.accept()
        self._clients.add(websocket)

    @property
    def client_count(self) -> int:
        """Number of currently connected WebSocket clients."""
        return len(self._clients)

    def unregister(self, websocket: WebSocket) -> None:
        """Stop tracking a disconnected WebSocket client."""
        self._clients.discard(websocket)

    async def _send_to_clients(self, message: dict[str, Any]) -> None:
        """Deliver one frame to every client concurrently, dropping dead sockets.

        Sends run in parallel with a per-client timeout: awaiting each socket in
        turn let one stalled browser block every other client's frames and the
        drain loop behind them.
        """
        clients = list(self._clients)
        if not clients:
            return

        async def send(client: WebSocket) -> bool:
            try:
                await asyncio.wait_for(
                    client.send_json(message), timeout=CLIENT_SEND_TIMEOUT)
            except asyncio.TimeoutError:
                logger.warning("Dropping WebSocket client that stalled for %.0fs",
                               CLIENT_SEND_TIMEOUT)
                return False
            except Exception:
                return False
            return True

        results = await asyncio.gather(*(send(client) for client in clients),
                                       return_exceptions=True)
        for client, ok in zip(clients, results):
            if ok is not True:
                self._clients.discard(client)

    async def broadcaster(self) -> None:
        """Background task that delivers queued messages to all clients."""
        while True:
            first = await self._queue.get()
            drained: list[dict[str, Any]] = [first]
            # Cap the drain so a flood becomes several bounded event_batch
            # frames rather than one enormous one.
            while len(drained) < MAX_COALESCED_EVENTS:
                try:
                    drained.append(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            for message in coalesce_broadcast_messages(drained):
                await self._send_to_clients(message)


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
    """Background task that periodically deletes expired runs.

    The sweep itself is synchronous SQLite work (two statements and a commit per
    expired run), so it runs in a worker thread: on the event loop a large sweep
    stalled every WebSocket client for its whole duration.
    """
    while True:
        try:
            count = await asyncio.to_thread(
                _run_retention_sweep, store, subscriber, manager, retention_days)
            if count:
                logger.info("Retention sweep deleted %s run(s)", count)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Retention sweep failed: %s", exc)
        await asyncio.sleep(interval)


def _run_stale_sweep(store: DashboardStore, manager: ConnectionManager,
                     stale_after: float,
                     subscriber: MqttSubscriber | None = None) -> tuple[int, int]:
    """Mark quiet named runs stale, delete unnamed stubs, and notify browsers.

    Returns ``(stale_count, deleted_count)``.
    """
    stale_ids, deleted_ids = store.mark_stale_runs(stale_after)
    for run_id in stale_ids:
        if subscriber is not None:
            subscriber.clear_retained(run_id)
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
    for run_id in deleted_ids:
        if subscriber is not None:
            subscriber.clear_retained(run_id)
        manager.submit_from_thread({
            "type": "run_deleted",
            "run_id": run_id,
        })
    return (len(stale_ids), len(deleted_ids))


async def _stale_sweeper(store: DashboardStore, manager: ConnectionManager,
                         stale_after: float, interval: float,
                         subscriber: MqttSubscriber | None = None) -> None:
    """Background task that marks quiet named builds as stale and deletes unnamed stubs.

    Always runs; *stale_after* / *interval* are expected to be positive (config
    clamps non-positive values). Clearing retained meta for stale named runs
    prevents broker retain from reviving them as ``running`` on reconnect.
    A later live MQTT message can still move the row back to ``running``.

    Like the retention sweep, the SQLite work happens in a worker thread.
    """
    while True:
        try:
            await asyncio.to_thread(
                _run_stale_sweep, store, manager, stale_after, subscriber)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Stale sweep failed: %s", exc)
        await asyncio.sleep(interval)


def _authorize(config: DashboardConfig, supplied: str | None) -> bool:
    """Return True when *supplied* matches the configured token (or none is set)."""
    if not config.auth_token:
        return True
    if not supplied:
        return False
    return hmac.compare_digest(str(supplied), config.auth_token)


def _token_from_request(request: Request) -> str | None:
    """Extract a token from the Authorization header, X-Dashboard-Token, or query."""
    header = request.headers.get("authorization") or ""
    if header.lower().startswith("bearer "):
        return header[7:].strip()
    return request.headers.get("x-dashboard-token") or request.query_params.get("token")


def create_app(config: DashboardConfig | None = None) -> FastAPI:
    """Build and configure the dashboard FastAPI application."""
    config = config or load_config()
    if config.http_host not in _LOOPBACK_HOSTS and not config.auth_token:
        logger.warning(
            "Dashboard is bound to %s with no NORNIR_DASHBOARD_TOKEN: any host "
            "that can reach the port can read and delete build history. Set a "
            "token, or set NORNIR_DASHBOARD_ALLOW_DELETE=0 to at least block "
            "deletion.", config.http_host)
    if not os.path.isfile(_INDEX_HTML):
        logger.error(
            "Dashboard static assets are missing (%s); the API will serve but "
            "the UI will not load", _INDEX_HTML)

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
        if config.retention_days > 0:
            retention_task = asyncio.create_task(
                _retention_sweeper(
                    store, subscriber, manager,
                    retention_days=config.retention_days,
                    interval=config.retention_sweep_interval,
                )
            )
        # Stale sweep is always enabled (config rejects <=0).
        stale_task = asyncio.create_task(
            _stale_sweeper(
                store, manager,
                stale_after=config.stale_after_seconds,
                interval=config.stale_sweep_interval,
                subscriber=subscriber,
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
            stale_task.cancel()
            store.close()

    def require_token(request: Request) -> None:
        """Reject API requests without the configured token (no-op when unset)."""
        if not _authorize(config, _token_from_request(request)):
            raise HTTPException(status_code=401, detail="invalid_or_missing_token")

    app = FastAPI(title="Nornir Build Dashboard", lifespan=lifespan)
    app.state.store = store
    app.state.config = config
    app.state.subscriber = subscriber
    app.state.manager = manager

    api_auth = [Depends(require_token)]

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        """Liveness probe for Compose/Docker HEALTHCHECK.

        Deliberately unauthenticated and free of database work so a health probe
        needs no token and cannot be slowed by a busy store.
        """
        return {"ok": True, "clients": manager.client_count}

    @app.get("/api/runs", dependencies=api_auth)
    def list_runs(limit: int = 200) -> dict[str, Any]:
        """Return summaries: active runs first (by activity, then start), then inactive by start."""
        return {"runs": store.list_runs(limit=clamp_runs_limit(limit))}

    @app.get("/api/runs/{run_id}", dependencies=api_auth)
    def get_run(run_id: str) -> dict[str, Any]:
        """Return a single run summary (``run`` is null when the run is unknown)."""
        return {"run": store.get_run(run_id)}

    @app.delete("/api/runs/{run_id}", dependencies=api_auth)
    def delete_run(run_id: str) -> JSONResponse:
        """Delete a run and notify live clients."""
        if not config.allow_delete:
            return JSONResponse(
                {"ok": False, "error": "delete_disabled"}, status_code=403)
        deleted = _delete_run_and_notify(store, subscriber, manager, run_id)
        if not deleted:
            return JSONResponse({"ok": False, "error": "not_found"}, status_code=404)
        return JSONResponse({"ok": True, "run_id": run_id})

    @app.get("/api/runs/{run_id}/events", dependencies=api_auth)
    def get_events(
        run_id: str,
        after_id: int = 0,
        before_id: int = 0,
        limit: int = EVENTS_LIMIT_DEFAULT,
        q: str = "",
        types: str = "",
    ) -> dict[str, Any]:
        """Return a page of events for a run.

        Without cursors, returns the newest ``limit`` matching rows. Use
        ``after_id`` for newer pages, ``before_id`` for older. ``q`` is a
        case-insensitive substring of the JSON payload; ``types`` is a
        comma-separated list of UI filter keys (error, warning, info, debug,
        event, status).
        """
        limit = clamp_events_limit(limit)
        query = q.strip() or None
        type_list = parse_types_param(types or None)
        return {
            "events": store.get_events(
                run_id,
                after_id=after_id,
                before_id=before_id,
                limit=limit,
                q=query,
                types=type_list,
            ),
            "limit": limit,
            "limit_max": EVENTS_LIMIT_MAX,
        }

    @app.get("/api/runs/{run_id}/events/export", dependencies=api_auth)
    def export_events(run_id: str, q: str = "", types: str = "") -> StreamingResponse:
        """Stream retained events for a run as plain text (oldest first)."""
        query = q.strip() or None
        type_list = parse_types_param(types or None)

        def _lines() -> Iterator[str]:
            for event in store.iter_events_for_export(run_id, q=query, types=type_list):
                payload = event.get("payload") or {}
                message = payload.get("message")
                if message is None and event.get("kind") == "event":
                    bits = [str(payload.get("event") or "event")]
                    if payload.get("function"):
                        bits.append(str(payload["function"]))
                    if payload.get("error"):
                        bits.append(str(payload["error"]))
                    message = " ".join(bits)
                elif message is None:
                    message = json.dumps(payload, default=str)
                level = event.get("level") or event.get("kind") or ""
                ts = event.get("ts") or 0
                yield f"{ts}\t{level}\t{message}\n"

        filename = f"nornir-run-{run_id}.log"
        return StreamingResponse(
            _lines(),
            media_type="text/plain; charset=utf-8",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
            },
        )

    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket) -> None:
        supplied = (websocket.headers.get("x-dashboard-token")
                    or websocket.query_params.get("token"))
        if not _authorize(config, supplied):
            await websocket.close(code=1008)
            return

        await manager.register(websocket)
        try:
            while True:
                # We do not expect inbound messages; this keeps the socket open
                # and detects client disconnects.
                await websocket.receive_text()
        except WebSocketDisconnect:
            pass
        except Exception:
            pass
        finally:
            # asyncio.CancelledError is a BaseException, so unregistering in
            # except clauses leaked sockets into later broadcasts on shutdown.
            manager.unregister(websocket)

    @app.get("/", response_model=None)
    def index() -> Response:
        """Serve the dashboard single-page app."""
        if not os.path.isfile(_INDEX_HTML):
            return JSONResponse(
                {"error": "static_assets_missing", "path": _INDEX_HTML},
                status_code=503)
        return FileResponse(_INDEX_HTML)

    if os.path.isdir(_STATIC_DIR):
        app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")

    return app


def __getattr__(name: str) -> Any:
    """Build the ASGI app on first access to ``main.app``.

    Kept for ``uvicorn nornir_dashboard.main:app``. Constructing the app at
    import time opened SQLite and created directories as a side effect of merely
    importing the package, which left stray database files wherever a test or
    tool happened to be running.
    """
    if name == "app":
        application = create_app()
        globals()["app"] = application
        return application
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def _setup_logging() -> None:
    """Configure logging, joining the unified Nornir log session when available.

    The dashboard image deliberately does not depend on the monorepo, so
    ``nornir_shared`` is usually absent inside the container and stdout logging
    (captured by Docker) is the only sink. When the package *is* importable and
    ``NORNIR_LOG_ROOT`` is set — running the dashboard from a working copy — the
    shared ``SetupLogging`` owns file log placement and naming, per the unified
    logging convention, instead of this module inventing its own paths.
    """
    if os.environ.get("NORNIR_LOG_ROOT"):
        try:
            from nornir_shared.misc import SetupLogging
        except ImportError:
            logger.debug(
                "NORNIR_LOG_ROOT is set but nornir_shared is not installed; "
                "logging to stdout only")
        else:
            SetupLogging(Level=logging.INFO)
            return
    logging.basicConfig(level=logging.INFO)


def run() -> None:
    """Console-script entry point: serve the dashboard with uvicorn."""
    import uvicorn

    _setup_logging()
    config = load_config()
    uvicorn.run(create_app(config), host=config.http_host, port=config.http_port)


if __name__ == "__main__":
    run()
