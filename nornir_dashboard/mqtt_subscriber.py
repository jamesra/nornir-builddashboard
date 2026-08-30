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

from nornir_dashboard.store import CLEAR, DashboardStore

logger = logging.getLogger(__name__)

# QoS 1 for retained clears: at QoS 0 a clear issued while the broker link is
# down is dropped silently and the deleted run reappears on the next reconnect.
_RETAINED_CLEAR_QOS = 1

# Only the ``meta`` leaf is published with retain=True by
# nornir_shared.mqtt_telemetry, so it is the only leaf a clear has to target.
_RETAINED_LEAVES = ("meta",)

_TERMINAL_STATUSES = frozenset({"completed", "failed", "skipped", "stale"})

# High-churn telemetry: project into run summary + live WS, but do not append to
# the SQLite transcript. Otherwise iterate_progress floods prune away real errors.
_EPHEMERAL_EVENT_KINDS = frozenset({"progress", "meta"})
_EPHEMERAL_EVENT_TYPES = frozenset({
    "iterate_progress",
    "iterate_progress_complete",
    "pool_load",
})


def _coerce_number(value: Any, default: float) -> float:
    """Return *value* as a float, or *default* when it is missing or unparsable.

    Publisher payloads are JSON from another process, so numeric fields arrive
    as whatever the publisher put there. A single string ``depth`` used to make
    the track sort raise ``TypeError`` and lose the whole message.
    """
    if value is None:
        return default
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    if result != result or result in (float("inf"), float("-inf")):  # NaN / inf
        return default
    return result


def _coerce_depth(value: Any) -> int | float:
    """Normalize a publisher-supplied track depth to a number (default 0)."""
    depth = _coerce_number(value, 0.0)
    return int(depth) if depth.is_integer() else depth


def _is_chain_progress_track(track_id: str) -> bool:
    """True for sticky ``--then`` chain bars (``chain`` / ``pipeline:*``)."""
    tid = str(track_id)
    return tid == "chain" or tid.startswith("pipeline:")


def _should_persist_event(kind: str, payload: dict[str, Any]) -> bool:
    """Return False for high-churn kinds that must not displace log rows."""
    if kind in _EPHEMERAL_EVENT_KINDS:
        return False
    if kind == "event":
        event_type = payload.get("event")
        if isinstance(event_type, str) and event_type in _EPHEMERAL_EVENT_TYPES:
            return False
    return True


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
            # Published under the lock so a concurrent stop() cannot observe
            # _started=True with no thread to join.
            self._connect_thread = threading.Thread(
                target=self._connect_with_retry,
                name="dashboard-mqtt-connect",
                daemon=True,
            )
            thread = self._connect_thread
        thread.start()

    def _connect_with_retry(self) -> None:
        """Attempt MQTT connect until success or :meth:`stop` clears ``_started``."""
        delay = 1.0
        max_delay = 60.0
        while self._started:
            try:
                self._client.connect(self._host, self._port, self._keepalive)
                with self._lock:
                    if not self._started:
                        # stop() ran during connect(); do not start a network
                        # thread that nothing will ever shut down.
                        self._client.disconnect()
                        return
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

    def stop(self, join_timeout: float = 5.0) -> None:
        """Stop the network loop and disconnect from the broker.

        Joins the connect thread so a connect already in flight cannot start a
        paho network thread after shutdown has run.
        """
        with self._lock:
            self._started = False
            thread = self._connect_thread
        self._stop_event.set()

        if thread is not None and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=join_timeout)
            if thread.is_alive():  # pragma: no cover - blocked in connect()
                logger.warning(
                    "MQTT connect thread did not exit within %.0fs; "
                    "continuing shutdown", join_timeout)

        try:
            self._client.loop_stop()
            self._client.disconnect()
        except Exception:  # pragma: no cover - best effort shutdown
            pass

    def clear_retained(self, run_id: str) -> bool:
        """Clear retained state for a deleted run; return True when published.

        Publishes at QoS 1 and checks the result: a dropped clear resurrects the
        deleted run as ``running`` from broker retain on the next reconnect.
        """
        if not run_id:
            return False
        published = True
        for leaf in _RETAINED_LEAVES:
            topic = f"{self._topic_root}/{run_id}/{leaf}"
            try:
                info = self._client.publish(
                    topic, payload=b"", qos=_RETAINED_CLEAR_QOS, retain=True)
            except Exception as exc:  # pragma: no cover - network dependent
                logger.warning("Failed to clear retained %s for %s: %s",
                               leaf, run_id, exc)
                published = False
                continue
            rc = getattr(info, "rc", mqtt.MQTT_ERR_SUCCESS)
            if rc != mqtt.MQTT_ERR_SUCCESS:
                logger.warning(
                    "Retained clear for %s was not accepted by the broker "
                    "(rc=%s); the run may reappear after reconnect", topic, rc)
                published = False
        return published

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
        ts = self._coerce_timestamp(topic, payload, now)

        kind, level = self._classify(leaf)
        # Info/debug log floods do not change sidebar counters; skip the full
        # run summary read+broadcast payload so WS stays usable under load.
        include_run = kind != "log" or level in ("error", "warning")
        run_summary: dict[str, Any] | None = None
        event_id = 0

        # One transaction per message: at SQLite defaults each store call was a
        # separate fsync, and it also makes the read-modify-write track merges
        # atomic against the sweepers running on the event loop.
        with self._store.transaction():
            # ensure_run's ON CONFLICT already writes last_seen, so no separate
            # last_seen UPDATE is needed here.
            self._store.ensure_run(run_id, now=now)

            # Live traffic can revive a stale row unless this payload asserts a
            # terminal status (completed / failed / skipped / stale).
            incoming_status = payload.get("status")
            if incoming_status not in _TERMINAL_STATUSES:
                if self._store.get_run_status(run_id) == "stale":
                    self._store.update_run_fields(
                        run_id, {"status": "running", "end_ts": CLEAR})

            self._project_state(run_id, kind, level, payload)

            if _should_persist_event(kind, payload):
                event_id = self._store.add_event(run_id, ts, kind, level, payload)

            if include_run:
                run_summary = self._store.get_run(run_id)

        message: dict[str, Any] = {
            "type": "event",
            "event": {
                "id": event_id,
                "run_id": run_id,
                "ts": ts,
                "kind": kind,
                "level": level,
                "payload": payload,
            },
        }
        if include_run:
            message["run"] = run_summary
        self._broadcast(message)

    @staticmethod
    def _coerce_timestamp(topic: str, payload: dict[str, Any], now: float) -> float:
        """Return the payload timestamp, falling back to *now* on bad input.

        A malformed ``ts`` must not discard the message: this used to raise
        before any store write, so an error line with a bad timestamp vanished
        from both the log pane and the sidebar counter.
        """
        for key in ("ts", "timestamp"):
            raw = payload.get(key)
            if not raw:
                continue
            try:
                return float(raw)
            except (TypeError, ValueError):
                logger.warning(
                    "Ignoring unparsable %s=%r on %s; using arrival time",
                    key, raw, topic)
        return now

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
            self._project_meta(run_id, payload)
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

    def _project_meta(self, run_id: str, payload: dict[str, Any]) -> None:
        """Project retained/ephemeral meta onto the run row, clearing progress when needed."""
        incoming_pipeline = payload.get("pipeline")
        if isinstance(incoming_pipeline, str):
            incoming_pipeline = incoming_pipeline.strip() or None

        existing = self._store.get_run(run_id)
        existing_pipeline = None
        if existing is not None:
            existing_pipeline = existing.get("pipeline")
            if isinstance(existing_pipeline, str):
                existing_pipeline = existing_pipeline.strip() or None

        status = payload.get("status")
        if status in _TERMINAL_STATUSES:
            self._store.clear_run_progress(run_id)
        elif (incoming_pipeline is not None
              and existing_pipeline is not None
              and incoming_pipeline != existing_pipeline):
            # --then chain segment (same run_id, new PipelineName): drop nested
            # iterate bars but keep depth-0 chain / pipeline:* tracks.
            self._clear_in_pipeline_progress(run_id)

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

    def _clear_in_pipeline_progress(self, run_id: str) -> None:
        """Remove in-pipeline progress tracks; keep sticky chain/pipeline bars.

        Also clears pool load tracks (they are stage-scoped load snapshots).
        """
        self._store.update_run_fields(run_id, {"pool_tracks": {}})
        tracks = self._store.get_progress_tracks(run_id)
        if not tracks:
            return
        kept = {
            tid: track for tid, track in tracks.items()
            if _is_chain_progress_track(tid)
        }
        if len(kept) == len(tracks):
            return
        if not kept:
            # Preserve pool clear already applied; still clear progress columns.
            self._store.clear_run_progress(run_id)
            return
        self._store.update_run_fields(run_id, {"progress_tracks": kept})
        self._refresh_top_level_progress(run_id)

    def _project_event(self, run_id: str, payload: dict[str, Any]) -> None:
        """Project a structured pipeline event onto the run summary."""
        event_type = payload.get("event")
        fields: dict[str, Any] = {}

        if event_type in ("stage_start", "stage_end", "stage_failed"):
            module = payload.get("module", "")
            function = payload.get("function", "")
            new_stage = f"{module}.{function}".strip(".")
            if event_type == "stage_start":
                existing = self._store.get_run(run_id)
                existing_stage = None if existing is None else existing.get("current_stage")
                if existing_stage and existing_stage != new_stage:
                    # Drop sticky nested tracks when Current Stage / Command changes;
                    # keep --then chain / pipeline:* bars.
                    self._clear_in_pipeline_progress(run_id)
            fields["current_stage"] = new_stage
            if payload.get("element") is not None:
                fields["current_element"] = payload.get("element")
            if payload.get("section") is not None:
                fields["current_section"] = str(payload.get("section"))
            if payload.get("path") is not None:
                fields["current_path"] = payload.get("path")
            if event_type == "stage_failed":
                # PipelineManager raises PipelineError right after publishing
                # this, so the run really is over; without a status write the
                # sidebar kept rendering it as running until unrelated meta
                # happened to arrive. error_count is deliberately not touched:
                # the same failure is also published as a log/error line, which
                # already increments the counter.
                fields["status"] = "failed"
                if payload.get("end_ts") is not None:
                    fields["end_ts"] = payload.get("end_ts")

        if event_type == "iterate_progress":
            if payload.get("section") is not None:
                fields["current_section"] = str(payload.get("section"))
            if payload.get("element") is not None:
                fields["current_element"] = payload.get("element")
            if payload.get("path") is not None:
                fields["current_path"] = payload.get("path")
            merged = self._merge_progress_track(run_id, payload)
            # Refresh top-level progress from shallowest largest track after merge.
            self._refresh_top_level_progress(run_id, tracks=merged)

        if event_type == "iterate_progress_complete":
            # Keep the last track snapshot sticky until current_stage changes
            # (or terminal / pipeline rename clears progress). Avoids ChannelNode
            # flicker between sections of the same stage.
            self._refresh_top_level_progress(run_id)

        if event_type == "pool_load":
            self._merge_pool_track(run_id, payload)

        if fields:
            self._store.update_run_fields(run_id, fields)

    def _merge_pool_track(self, run_id: str, payload: dict[str, Any]) -> None:
        """Merge a ``pool_load`` event into the run ``pool_tracks`` map."""
        name = payload.get("name") or payload.get("label") or "pool"
        key = str(name)
        queued = payload.get("queued")
        active = payload.get("active")
        outstanding = payload.get("outstanding")
        if outstanding is None:
            try:
                q = int(queued or 0)
                a = int(active) if active is not None else 0
                outstanding = q + a if active is not None else q
            except (TypeError, ValueError):
                outstanding = 0
        tracks = dict(self._store.get_pool_tracks(run_id))
        entry: dict[str, Any] = {
            "label": key,
            "queued": queued,
            "outstanding": outstanding,
        }
        if active is not None:
            entry["active"] = active
        if payload.get("max_workers") is not None:
            entry["max_workers"] = payload.get("max_workers")
        tracks[key] = entry
        self._store.update_run_fields(run_id, {"pool_tracks": tracks})

    def _merge_progress_track(self, run_id: str,
                              payload: dict[str, Any]) -> dict[str, Any]:
        """Merge an iterate_progress or labeled progress update into progress_tracks.

        Returns the merged track map so the caller can refresh top-level
        progress without re-reading and re-decoding the blob.

        ``label`` is the stable track title (pipeline VariableName). ``element``
        and ``section`` are the current item; an event that omits them keeps the
        last values so nested iterates do not flicker on each parent restart.
        """
        track_id = payload.get("track_id") or payload.get("label") or "progress"
        label = payload.get("label") or str(track_id)
        current = payload.get("current")
        if current is None:
            current = payload.get("progress")
        total = payload.get("total")
        depth = _coerce_depth(payload.get("depth"))

        fraction = payload.get("fraction")
        if fraction is None and current is not None and total:
            try:
                fraction = float(current) / float(total)
            except (TypeError, ZeroDivisionError, ValueError):
                fraction = None

        tracks = dict(self._store.get_progress_tracks(run_id))
        existing = tracks.get(str(track_id))
        if not isinstance(existing, dict):
            existing = {}

        element = payload.get("element")
        if element is None:
            element = existing.get("element")
        section = payload.get("section")
        if section is None:
            section = existing.get("section")

        entry: dict[str, Any] = {
            "label": label,
            "depth": depth,
            "current": current,
            "total": total,
            "fraction": fraction,
        }
        if element is not None:
            entry["element"] = element
        if section is not None:
            entry["section"] = section
        tracks[str(track_id)] = entry
        self._store.update_run_fields(run_id, {"progress_tracks": tracks})
        return tracks

    def _refresh_top_level_progress(self, run_id: str,
                                    tracks: dict[str, Any] | None = None) -> None:
        """Set sidebar progress from the shallowest active track with the largest total.

        Callers that just merged a track pass the merged map in *tracks* to
        avoid decoding the blob a second time.
        """
        if tracks is None:
            tracks = self._store.get_progress_tracks(run_id)
        if not tracks:
            self._store.clear_run_progress(run_id)
            return

        def sort_key(item: tuple[str, Any]) -> tuple:
            track = item[1] if isinstance(item[1], dict) else {}
            # Both keys are publisher-supplied and may be strings; comparing a
            # str depth against an int depth would raise mid-projection.
            depth_val = _coerce_number(track.get("depth"), 999.0)
            total_val = _coerce_number(track.get("total"), 0.0)
            return (depth_val, -total_val)

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
        merged_tracks: dict[str, Any] | None = None
        if payload.get("label"):
            # Operation-level labeled progress joins the nested track stack.
            labeled = dict(payload)
            if "track_id" not in labeled:
                labeled["track_id"] = f"op:{payload.get('label')}"
            if "depth" not in labeled:
                labeled["depth"] = 100  # deeper than iterate tracks by default
            if "current" not in labeled and payload.get("progress") is not None:
                labeled["current"] = payload.get("progress")
            merged_tracks = self._merge_progress_track(run_id, labeled)

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
        if merged_tracks is not None:
            self._refresh_top_level_progress(run_id, tracks=merged_tracks)
