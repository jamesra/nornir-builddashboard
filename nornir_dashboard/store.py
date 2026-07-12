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
import json
import os
import sqlite3
import threading
import time
from typing import Any


_RUN_COLUMNS = (
    "run_id", "pipeline", "volumepath", "host", "pid", "session_id",
    "status", "start_ts", "end_ts", "first_seen", "last_seen",
    "error_count", "warning_count", "current_stage", "current_element",
    "current_section", "progress_current", "progress_total", "progress_fraction",
)


class DashboardStore:
    """Thread-safe SQLite store for runs and their event streams."""

    _connection: sqlite3.Connection
    _lock: threading.Lock
    _max_events_per_run: int

    def __init__(self, database_path: str, max_events_per_run: int = 5000) -> None:
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
                    progress_fraction REAL
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
        updates = {k: v for k, v in fields.items() if k in _RUN_COLUMNS and k != "run_id"}
        if not updates:
            return

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

    def list_runs(self, limit: int = 200) -> list[dict[str, Any]]:
        """Return run summaries, most recently active first."""
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM runs
                ORDER BY COALESCE(last_seen, first_seen, 0) DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        """Return a single run summary, or None when unknown."""
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM runs WHERE run_id=?", (run_id,)
            ).fetchone()
        return dict(row) if row is not None else None

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

    def get_events(self, run_id: str, after_id: int = 0,
                   limit: int = 2000) -> list[dict[str, Any]]:
        """Return events for a run with id greater than ``after_id``."""
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT id, run_id, ts, kind, level, payload FROM events
                WHERE run_id=? AND id > ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (run_id, after_id, limit),
            ).fetchall()

        events: list[dict[str, Any]] = []
        for row in rows:
            event = dict(row)
            try:
                event["payload"] = json.loads(event["payload"]) if event["payload"] else {}
            except (TypeError, ValueError):
                event["payload"] = {}
            events.append(event)
        return events

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
