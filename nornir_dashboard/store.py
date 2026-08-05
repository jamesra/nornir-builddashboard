"""SQLite-backed persistence for dashboard runs and events.

Two tables are maintained:

* ``runs``   - one row per nornir-build run (keyed by ``run_id``), holding the
  latest known summary state used by the run list and detail header.
* ``events`` - the append-only stream of messages (logs, stage events,
  progress, status) for each run, used to populate the detail view and to
  reconstruct history for runs that completed before the browser connected.

A single connection is shared across the MQTT subscriber thread and the
FastAPI request handlers, guarded by a lock (SQLite connections are not safe
for concurrent use from multiple threads).
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from collections.abc import Iterator
from typing import Any


_RUN_COLUMNS = (
    "run_id", "pipeline", "volumepath", "host", "pid", "session_id",
    "status", "start_ts", "end_ts", "first_seen", "last_seen",
    "error_count", "warning_count", "current_stage", "current_element",
    "current_section", "current_path", "progress_current", "progress_total",
    "progress_fraction", "compute", "progress_tracks",
)

_ADDED_COLUMNS = (
    ("compute", "TEXT"),
    ("progress_tracks", "TEXT"),
    ("current_path", "TEXT"),
)

# Matches UI .lvl checkbox values in static/index.html / app.js logFilterKey().
LOG_FILTER_TYPES = frozenset({"error", "warning", "info", "debug", "event", "status"})

EVENTS_LIMIT_MAX = 5000
EVENTS_LIMIT_DEFAULT = 2000


def clamp_events_limit(limit: int) -> int:
    """Clamp a per-request event page size to a safe range."""
    if limit < 1:
        return 1
    if limit > EVENTS_LIMIT_MAX:
        return EVENTS_LIMIT_MAX
    return limit


def parse_types_param(types: str | list[str] | None) -> list[str] | None:
    """Parse a comma-separated types query into known filter keys, or None for all."""
    if types is None:
        return None
    if isinstance(types, str):
        raw = [part.strip().lower() for part in types.split(",") if part.strip()]
    else:
        raw = [str(part).strip().lower() for part in types if str(part).strip()]
    if not raw:
        return None
    return [t for t in raw if t in LOG_FILTER_TYPES]


def _types_sql(types: list[str] | None) -> tuple[str, list[Any]]:
    """Build a SQL predicate matching UI log filter keys.

    ``None`` means no type filter (all kinds). An empty list matches nothing.
    """
    if types is None:
        return "1=1", []
    if not types:
        return "0=1", []
    parts: list[str] = []
    params: list[Any] = []
    for t in types:
        if t in ("error", "warning", "debug"):
            parts.append("(kind = 'log' AND lower(COALESCE(level, '')) = ?)")
            params.append(t)
        elif t == "info":
            parts.append(
                "(kind = 'log' AND (level IS NULL OR level = '' OR lower(level) = 'info'))"
            )
        elif t == "event":
            parts.append("kind = 'event'")
        elif t == "status":
            parts.append("kind = 'status'")
    if not parts:
        return "0=1", []
    return "(" + " OR ".join(parts) + ")", params


class DashboardStore:
    """Thread-safe SQLite store for runs and their event streams."""

    _connection: sqlite3.Connection
    _lock: threading.Lock
    _max_events_per_run: int

    def __init__(self, database_path: str, max_events_per_run: int = 100000) -> None:
        if database_path != ":memory:":
            parent = os.path.dirname(os.path.abspath(database_path))
            os.makedirs(parent, exist_ok=True)

        self._connection = sqlite3.connect(database_path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._max_events_per_run = max_events_per_run
        self._initialize_schema()

    def _initialize_schema(self) -> None:
        """Create tables and indexes when they do not already exist."""
        with self._lock:
            self._connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    pipeline TEXT,
                    volumepath TEXT,
                    host TEXT,
                    pid INTEGER,
                    session_id TEXT,
                    status TEXT,
                    start_ts REAL,
                    end_ts REAL,
                    first_seen REAL,
                    last_seen REAL,
                    error_count INTEGER DEFAULT 0,
                    warning_count INTEGER DEFAULT 0,
                    current_stage TEXT,
                    current_element TEXT,
                    current_section TEXT,
                    progress_current INTEGER,
                    progress_total INTEGER,
                    progress_fraction REAL,
                    compute TEXT,
                    progress_tracks TEXT
                );

                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    ts REAL,
                    kind TEXT,
                    level TEXT,
                    payload TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_events_run ON events(run_id, id);
                """
            )
            existing = {
                row[1]
                for row in self._connection.execute("PRAGMA table_info(runs)").fetchall()
            }
            for column, column_type in _ADDED_COLUMNS:
                if column not in existing:
                    self._connection.execute(
                        f"ALTER TABLE runs ADD COLUMN {column} {column_type}"
                    )
            self._connection.commit()

    # -- run helpers ------------------------------------------------------

    def ensure_run(self, run_id: str, now: float | None = None) -> None:
        """Insert a placeholder run row when the run id is seen for the first time."""
        if not run_id:
            return
        now = time.time() if now is None else now
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO runs (run_id, status, first_seen, last_seen)
                VALUES (?, 'running', ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET last_seen=excluded.last_seen
                """,
                (run_id, now, now),
            )
            self._connection.commit()

    def update_run_fields(self, run_id: str, fields: dict[str, Any]) -> None:
        """Update the given summary columns for a run, ignoring unknown keys."""
        updates = {
            k: v for k, v in fields.items()
            if k in _RUN_COLUMNS and k != "run_id" and v is not None
        }
        if not updates:
            return

        if "progress_tracks" in updates and not isinstance(updates["progress_tracks"], str):
            updates["progress_tracks"] = json.dumps(updates["progress_tracks"], default=str)

        assignments = ", ".join(f"{column}=?" for column in updates)
        values = list(updates.values())
        values.append(run_id)
        with self._lock:
            self._connection.execute(
                f"UPDATE runs SET {assignments} WHERE run_id=?", values
            )
            self._connection.commit()

    def increment_counter(self, run_id: str, column: str) -> None:
        """Atomically increment ``error_count`` or ``warning_count`` for a run."""
        if column not in ("error_count", "warning_count"):
            return
        with self._lock:
            self._connection.execute(
                f"UPDATE runs SET {column}=COALESCE({column},0)+1 WHERE run_id=?",
                (run_id,),
            )
            self._connection.commit()

    @staticmethod
    def _decode_run_row(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        """Convert a runs row into a JSON-friendly dict with parsed progress_tracks."""
        result = dict(row)
        raw_tracks = result.get("progress_tracks")
        if isinstance(raw_tracks, str) and raw_tracks:
            try:
                result["progress_tracks"] = json.loads(raw_tracks)
            except (TypeError, ValueError):
                result["progress_tracks"] = {}
        elif raw_tracks is None:
            result["progress_tracks"] = {}
        return result

    def list_runs(self, limit: int = 200) -> list[dict[str, Any]]:
        """Return run summaries: active by last activity then start, then inactive by start."""
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM runs
                ORDER BY
                  CASE
                    WHEN COALESCE(status, 'running') IN ('completed', 'failed', 'skipped', 'stale')
                    THEN 1 ELSE 0
                  END ASC,
                  CASE
                    WHEN COALESCE(status, 'running') IN ('completed', 'failed', 'skipped', 'stale')
                    THEN 0
                    ELSE COALESCE(last_seen, first_seen, 0)
                  END DESC,
                  COALESCE(start_ts, first_seen, 0) DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._decode_run_row(row) for row in rows]

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        """Return a single run summary, or None when unknown."""
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM runs WHERE run_id=?", (run_id,)
            ).fetchone()
        return self._decode_run_row(row) if row is not None else None

    def get_progress_tracks(self, run_id: str) -> dict[str, Any]:
        """Return the progress_tracks map for a run (empty dict when unset)."""
        run = self.get_run(run_id)
        if run is None:
            return {}
        tracks = run.get("progress_tracks") or {}
        return tracks if isinstance(tracks, dict) else {}

    def delete_run(self, run_id: str) -> bool:
        """Delete a run and its events. Return True when a row was removed."""
        with self._lock:
            cursor = self._connection.execute(
                "DELETE FROM runs WHERE run_id=?", (run_id,)
            )
            self._connection.execute("DELETE FROM events WHERE run_id=?", (run_id,))
            self._connection.commit()
            return cursor.rowcount > 0

    def list_expired_run_ids(self, older_than_days: float, now: float | None = None
                             ) -> list[str]:
        """Return run ids whose last_seen is older than the retention window."""
        if older_than_days <= 0:
            return []
        now = time.time() if now is None else now
        cutoff = now - (older_than_days * 86400.0)
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT run_id FROM runs
                WHERE COALESCE(last_seen, first_seen, 0) < ?
                """,
                (cutoff,),
            ).fetchall()
        return [str(row["run_id"]) for row in rows]

    def mark_stale_runs(self, stale_after_seconds: float, now: float | None = None
                        ) -> list[str]:
        """Mark running runs as stale when they have not sent traffic recently."""
        if stale_after_seconds <= 0:
            return []
        now = time.time() if now is None else now
        cutoff = now - stale_after_seconds
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT run_id FROM runs
                WHERE status='running' AND COALESCE(last_seen, first_seen, 0) < ?
                """,
                (cutoff,),
            ).fetchall()
            run_ids = [str(row["run_id"]) for row in rows]
            for run_id in run_ids:
                self._connection.execute(
                    "UPDATE runs SET status=? WHERE run_id=?",
                    ("stale", run_id),
                )
            self._connection.commit()
        return run_ids

    # -- event helpers ----------------------------------------------------

    def add_event(self, run_id: str, ts: float, kind: str, level: str | None,
                  payload: dict[str, Any]) -> int:
        """Append an event for a run and return its row id."""
        with self._lock:
            cursor = self._connection.execute(
                "INSERT INTO events (run_id, ts, kind, level, payload) VALUES (?, ?, ?, ?, ?)",
                (run_id, ts, kind, level, json.dumps(payload, default=str)),
            )
            event_id = int(cursor.lastrowid)
            self._connection.commit()
        return event_id

    @staticmethod
    def _decode_event_row(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        """Convert an events row into a JSON-friendly dict with parsed payload."""
        event = dict(row)
        try:
            event["payload"] = json.loads(event["payload"]) if event["payload"] else {}
        except (TypeError, ValueError):
            event["payload"] = {}
        return event

    def _event_filter_sql(
        self,
        run_id: str,
        after_id: int,
        before_id: int,
        q: str | None,
        types: list[str] | None,
    ) -> tuple[str, list[Any]]:
        """Shared WHERE clause for paginated and export event queries."""
        type_sql, type_params = _types_sql(types)
        clauses = ["run_id = ?", type_sql]
        params: list[Any] = [run_id, *type_params]
        if after_id > 0:
            clauses.append("id > ?")
            params.append(after_id)
        if before_id > 0:
            clauses.append("id < ?")
            params.append(before_id)
        if q:
            clauses.append("lower(COALESCE(payload, '')) LIKE ?")
            params.append(f"%{q.lower()}%")
        return " AND ".join(clauses), params

    def get_events(
        self,
        run_id: str,
        after_id: int = 0,
        before_id: int = 0,
        limit: int = EVENTS_LIMIT_DEFAULT,
        q: str | None = None,
        types: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Return a page of events for a run.

        With neither cursor set, returns the **newest** ``limit`` matching rows
        (ascending by id). ``after_id`` pages forward (newer); ``before_id``
        pages backward (older). ``q`` and ``types`` filter the result set.
        """
        limit = clamp_events_limit(limit)
        where_sql, params = self._event_filter_sql(run_id, after_id, before_id, q, types)
        # Newest page or older page: fetch DESC then reverse for stable ASC order.
        newest_or_older = after_id <= 0
        order = "DESC" if newest_or_older else "ASC"
        sql = (
            f"SELECT id, run_id, ts, kind, level, payload FROM events "
            f"WHERE {where_sql} ORDER BY id {order} LIMIT ?"
        )
        with self._lock:
            rows = self._connection.execute(sql, [*params, limit]).fetchall()
        events = [self._decode_event_row(row) for row in rows]
        if newest_or_older:
            events.reverse()
        return events

    def iter_events_for_export(
        self,
        run_id: str,
        q: str | None = None,
        types: list[str] | None = None,
        batch_size: int = 1000,
    ) -> Iterator[dict[str, Any]]:
        """Yield matching events oldest-first for streaming export."""
        batch_size = clamp_events_limit(batch_size)
        where_sql, params = self._event_filter_sql(run_id, 0, 0, q, types)
        last_id = 0
        while True:
            page_where = f"{where_sql} AND id > ?"
            sql = (
                f"SELECT id, run_id, ts, kind, level, payload FROM events "
                f"WHERE {page_where} ORDER BY id ASC LIMIT ?"
            )
            with self._lock:
                rows = self._connection.execute(
                    sql, [*params, last_id, batch_size]
                ).fetchall()
            if not rows:
                return
            for row in rows:
                event = self._decode_event_row(row)
                last_id = int(event["id"])
                yield event

    def prune_events(self, run_id: str) -> None:
        """Trim a run's event history to ``max_events_per_run`` newest rows."""
        if self._max_events_per_run <= 0:
            return
        with self._lock:
            self._connection.execute(
                """
                DELETE FROM events
                WHERE run_id=? AND id NOT IN (
                    SELECT id FROM events WHERE run_id=? ORDER BY id DESC LIMIT ?
                )
                """,
                (run_id, run_id, self._max_events_per_run),
            )
            self._connection.commit()

    def close(self) -> None:
        """Close the underlying SQLite connection."""
        with self._lock:
            self._connection.close()
