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
    _connect_thread: threading.Thread | None
    _stop_event: threading.Event

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
        self._connect_thread = None
        self._stop_event = threading.Event()

        self._client = mqtt.Client(callback_api_version=mqtt_enum.CallbackAPIVersion.VERSION2)
        self._client.reconnect_delay_set(min_delay=1, max_delay=60)
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

    def start(self) -> None:
        """Connect to the broker and begin processing messages in a background thread.

        Retries with exponential backoff when the broker is not yet reachable
        (for example a Compose ``depends_on`` race with Mosquitto).
        """
        with self._lock:
            if self._started:
                return
            self._started = True

        self._connect_thread = threading.Thread(
            target=self._connect_with_retry,
            name="dashboard-mqtt-connect",
            daemon=True,
        )
        self._connect_thread.start()

    def _connect_with_retry(self) -> None:
        """Attempt MQTT connect until success or :meth:`stop` clears ``_started``."""
        delay = 1.0
        max_delay = 60.0
        while self._started:
            try:
                self._client.connect(self._host, self._port, self._keepalive)
                self._client.loop_start()
                logger.info(
                    "Dashboard subscriber connecting to %s:%s (topic root %s)",
                    self._host, self._port, self._topic_root)
                return
            except Exception as exc:  # pragma: no cover - network dependent
                logger.error(
                    "Failed to connect to MQTT broker %s:%s: %s; retrying in %.0fs",
                    self._host, self._port, exc, delay)
                if self._stop_event.wait(timeout=delay):
                    return
                if not self._started:
                    return
                delay = min(delay * 2.0, max_delay)

    def stop(self) -> None:
        """Stop the network loop and disconnect from the broker."""
        with self._lock:
            self._started = False
        self._stop_event.set()
        try:
            self._client.loop_stop()
            self._client.disconnect()
        except Exception:  # pragma: no cover - best effort shutdown
            pass

    def clear_retained(self, run_id: str) -> None:
        """Clear retained meta for a deleted run by publishing an empty retained payload."""
        if not run_id:
            return
        topic = f"{self._topic_root}/{run_id}/meta"
        try:
            self._client.publish(topic, payload=b"", retain=True)
        except Exception as exc:  # pragma: no cover - network dependent
            logger.warning("Failed to clear retained meta for %s: %s", run_id, exc)

    def _on_connect(self, client: mqtt.Client, userdata: Any, flags: Any,
                    reason_code: Any, properties: Any = None) -> None:
        topic = f"{self._topic_root}/#"
        client.subscribe(topic)
        logger.info(
            "Subscribed to %s on %s:%s (reason=%s)",
            topic, self._host, self._port, reason_code)

    def _on_disconnect(self, client: mqtt.Client, userdata: Any, flags: Any,
                       reason_code: Any, properties: Any = None) -> None:
        logger.warning(
            "Disconnected from MQTT broker %s:%s (%s); paho will auto-reconnect",
            self._host, self._port, reason_code)

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

        # Empty retained clear for deleted runs — ignore.
        if not raw_payload:
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
                "compute": payload.get("compute"),
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
            self._merge_progress_track(run_id, payload)
            # Refresh top-level progress from shallowest largest track after merge.
            self._refresh_top_level_progress(run_id)

        if fields:
            self._store.update_run_fields(run_id, fields)

    def _merge_progress_track(self, run_id: str, payload: dict[str, Any]) -> None:
        """Merge an iterate_progress or labeled progress update into progress_tracks."""
        track_id = payload.get("track_id") or payload.get("label") or "progress"
        label = payload.get("label") or str(track_id)
        current = payload.get("current")
        if current is None:
            current = payload.get("progress")
        total = payload.get("total")
        depth = payload.get("depth")
        if depth is None:
            depth = 0

        fraction = payload.get("fraction")
        if fraction is None and current is not None and total:
            try:
                fraction = float(current) / float(total)
            except (TypeError, ZeroDivisionError, ValueError):
                fraction = None

        tracks = dict(self._store.get_progress_tracks(run_id))
        tracks[str(track_id)] = {
            "label": label,
            "depth": depth,
            "current": current,
            "total": total,
            "fraction": fraction,
        }
        self._store.update_run_fields(run_id, {"progress_tracks": tracks})

    def _refresh_top_level_progress(self, run_id: str) -> None:
        """Set sidebar progress from the shallowest active track with the largest total."""
        tracks = self._store.get_progress_tracks(run_id)
        if not tracks:
            return

        def sort_key(item: tuple[str, Any]) -> tuple:
            track = item[1] if isinstance(item[1], dict) else {}
            depth = track.get("depth", 999)
            total = track.get("total") or 0
            try:
                total_val = float(total)
            except (TypeError, ValueError):
                total_val = 0.0
            return (depth, -total_val)

        ordered = sorted(tracks.items(), key=sort_key)
        best = ordered[0][1] if ordered else None
        if not isinstance(best, dict):
            return

        fields: dict[str, Any] = {}
        if best.get("current") is not None:
            fields["progress_current"] = best.get("current")
        if best.get("total") is not None:
            fields["progress_total"] = best.get("total")
        if best.get("fraction") is not None:
            fields["progress_fraction"] = best.get("fraction")
        elif best.get("total"):
            try:
                fields["progress_fraction"] = float(best.get("current") or 0) / float(best["total"])
            except (TypeError, ZeroDivisionError, ValueError):
                pass
        if fields:
            self._store.update_run_fields(run_id, fields)

    def _update_progress(self, run_id: str, payload: dict[str, Any]) -> None:
        """Update progress columns from a CurseProgress-style payload."""
        if payload.get("label"):
            # Operation-level labeled progress joins the nested track stack.
            labeled = dict(payload)
            if "track_id" not in labeled:
                labeled["track_id"] = f"op:{payload.get('label')}"
            if "depth" not in labeled:
                labeled["depth"] = 100  # deeper than iterate tracks by default
            if "current" not in labeled and payload.get("progress") is not None:
                labeled["current"] = payload.get("progress")
            self._merge_progress_track(run_id, labeled)

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
        if payload.get("label"):
            self._refresh_top_level_progress(run_id)
