"""MQTT subscriber that feeds run/event state into the dashboard store.

Subscribes to the run-scoped topic tree (``nornir/run/#``) published by
``nornir_shared``. Each message is persisted to :class:`DashboardStore` and a
normalized update is handed to a broadcast callback for live WebSocket delivery.
"""
import json
import logging
import threading
import time
from typing import Any, Callable

import paho.mqtt.client as mqtt
import paho.mqtt.enums as mqtt_enum

from nornir_dashboard.store import DashboardStore

logger = logging.getLogger(__name__)

# Callback invoked (from the MQTT network thread) with a JSON-serializable dict
# describing a live update to broadcast to connected browsers.
BroadcastFn = Callable[[dict[str, Any]], None]


class MqttSubscriber:
    """Subscribe to run topics and project messages into the store."""

    _store: DashboardStore
    _host: str
    _port: int
    _keepalive: int
    _topic_root: str
    _broadcast: BroadcastFn
    _client: mqtt.Client
    _started: bool
    _lock: threading.Lock

    def __init__(self, store: DashboardStore, host: str, port: int, keepalive: int,
                 topic_root: str, broadcast: BroadcastFn) -> None:
        self._store = store
        self._host = host
        self._port = port
        self._keepalive = keepalive
        self._topic_root = topic_root.rstrip("/")
        self._broadcast = broadcast
        self._started = False
        self._lock = threading.Lock()

        self._client = mqtt.Client(callback_api_version=mqtt_enum.CallbackAPIVersion.VERSION2)
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message

    def start(self) -> None:
        """Connect to the broker and begin processing messages in a background thread."""
        with self._lock:
            if self._started:
                return
            self._started = True

        try:
            self._client.connect(self._host, self._port, self._keepalive)
            self._client.loop_start()
            logger.info("Dashboard subscriber connecting to %s:%s", self._host, self._port)
        except Exception as exc:  # pragma: no cover - network dependent
            logger.error("Failed to connect to MQTT broker %s:%s: %s",
                         self._host, self._port, exc)

    def stop(self) -> None:
        """Stop the network loop and disconnect from the broker."""
        try:
            self._client.loop_stop()
            self._client.disconnect()
        except Exception:  # pragma: no cover - best effort shutdown
            pass

    def _on_connect(self, client: mqtt.Client, userdata: Any, flags: Any,
                    reason_code: Any, properties: Any = None) -> None:
        topic = f"{self._topic_root}/#"
        client.subscribe(topic)
        logger.info("Subscribed to %s", topic)

    def _on_message(self, client: mqtt.Client, userdata: Any, message: Any) -> None:
        try:
            self._handle_message(message.topic, message.payload)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Error handling MQTT message on %s: %s", message.topic, exc)

    # -- message projection ----------------------------------------------

    def _handle_message(self, topic: str, raw_payload: bytes) -> None:
        run_id, leaf = self._parse_topic(topic)
        if run_id is None or leaf is None:
            return

        try:
            payload = json.loads(raw_payload.decode("utf-8")) if raw_payload else {}
        except (UnicodeDecodeError, ValueError):
            payload = {"message": raw_payload.decode("utf-8", errors="replace")}

        if not isinstance(payload, dict):
            payload = {"message": payload}

        now = time.time()
        ts = float(payload.get("ts") or payload.get("timestamp") or now)

        self._store.ensure_run(run_id, now=now)
        self._store.update_run_fields(run_id, {"last_seen": now})

        kind, level = self._classify(leaf)
        self._project_state(run_id, kind, level, payload)

        event_id = self._store.add_event(run_id, ts, kind, level, payload)
        self._store.prune_events(run_id)

        run_summary = self._store.get_run(run_id)

        self._broadcast({
            "type": "event",
            "event": {
                "id": event_id,
                "run_id": run_id,
                "ts": ts,
                "kind": kind,
                "level": level,
                "payload": payload,
            },
            "run": run_summary,
        })

    def _parse_topic(self, topic: str) -> tuple[str | None, str | None]:
        """Split a topic into ``(run_id, leaf)`` relative to the topic root."""
        prefix = self._topic_root + "/"
        if not topic.startswith(prefix):
            return (None, None)

        remainder = topic[len(prefix):]
        parts = remainder.split("/", 1)
        if len(parts) == 1:
            return (parts[0], "")
        return (parts[0], parts[1])

    @staticmethod
    def _classify(leaf: str) -> tuple[str, str | None]:
        """Return ``(kind, level)`` for a topic leaf such as ``log/error``."""
        if leaf == "meta":
            return ("meta", None)
        if leaf == "progress":
            return ("progress", None)
        if leaf == "status":
            return ("status", None)
        if leaf == "event":
            return ("event", None)
        if leaf.startswith("log/"):
            return ("log", leaf.split("/", 1)[1])
        return ("other", None)

    def _project_state(self, run_id: str, kind: str, level: str | None,
                       payload: dict[str, Any]) -> None:
        """Update the run summary columns based on a single message."""
        if kind == "meta":
            self._store.update_run_fields(run_id, {
                "pipeline": payload.get("pipeline"),
                "volumepath": payload.get("volumepath"),
                "host": payload.get("host"),
                "pid": payload.get("pid"),
                "session_id": payload.get("session_id"),
                "status": payload.get("status"),
                "start_ts": payload.get("start_ts"),
                "end_ts": payload.get("end_ts"),
            })
            return

        if kind == "log":
            if level == "error":
                self._store.increment_counter(run_id, "error_count")
            elif level == "warning":
                self._store.increment_counter(run_id, "warning_count")
            return

        if kind == "progress":
            self._update_progress(run_id, payload)
            return

        if kind == "event":
            self._project_event(run_id, payload)

    def _project_event(self, run_id: str, payload: dict[str, Any]) -> None:
        """Project a structured pipeline event onto the run summary."""
        event_type = payload.get("event")
        fields: dict[str, Any] = {}

        if event_type in ("stage_start", "stage_end", "stage_failed"):
            module = payload.get("module", "")
            function = payload.get("function", "")
            fields["current_stage"] = f"{module}.{function}".strip(".")
            if payload.get("element") is not None:
                fields["current_element"] = payload.get("element")
            if payload.get("section") is not None:
                fields["current_section"] = str(payload.get("section"))

        if event_type == "iterate_progress":
            if payload.get("section") is not None:
                fields["current_section"] = str(payload.get("section"))
            elif payload.get("element") is not None:
                fields["current_element"] = payload.get("element")
            current = payload.get("current")
            total = payload.get("total")
            if current is not None and total:
                fields["progress_current"] = current
                fields["progress_total"] = total
                try:
                    fields["progress_fraction"] = float(current) / float(total)
                except (TypeError, ZeroDivisionError, ValueError):
                    pass

        if fields:
            self._store.update_run_fields(run_id, fields)

    def _update_progress(self, run_id: str, payload: dict[str, Any]) -> None:
        """Update progress columns from a CurseProgress-style payload."""
        fields: dict[str, Any] = {}
        if payload.get("progress") is not None:
            fields["progress_current"] = payload.get("progress")
        if payload.get("total") is not None:
            fields["progress_total"] = payload.get("total")

        fraction = payload.get("fraction")
        if fraction is None and payload.get("total"):
            try:
                fraction = float(payload.get("progress")) / float(payload.get("total"))
            except (TypeError, ZeroDivisionError, ValueError):
                fraction = None
        if fraction is not None:
            fields["progress_fraction"] = fraction

        if fields:
            self._store.update_run_fields(run_id, fields)
