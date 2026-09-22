"""SQLite-backed persistence for dashboard runs and events.

Two tables are maintained:

* ``runs``   - one row per nornir-build run (keyed by ``run_id``), holding the
  latest known summary state used by the run list and detail header.
* ``events`` - the append-only stream of messages (logs, stage events,
  progress, status) for each run, used to populate the detail view and to
  reconstruct history for runs that completed before the browser connected.

A single connection is shared across the MQTT subscriber thread and the
FastAPI request handlers, guarded by a re-entrant lock (SQLite connections are
not safe for concurrent use from multiple threads). Callers that need several
writes to land together — or a read-modify-write to be atomic against the
sweepers — wrap them in :meth:`DashboardStore.transaction`, which collapses
them into one commit.
"""
from __future__ import annotations

import contextlib
import json
import logging
import os
import sqlite3
import threading
import time
from collections.abc import Iterator
from typing import Any

logger = logging.getLogger(__name__)


class _ClearSentinel:
    """Marker requesting an explicit SQL NULL, as opposed to "field not supplied"."""

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "CLEAR"

    def __bool__(self) -> bool:
        return False


# Callers project MQTT payloads with ``payload.get(...)``, so a bare ``None``
# has to keep meaning "absent, leave the column alone". CLEAR is the opt-in way
# to say "write NULL" (for example clearing end_ts when a stale run revives).
CLEAR = _ClearSentinel()


_RUN_COLUMNS = (
    "run_id", "pipeline", "volumepath", "host", "pid", "session_id",
    "status", "start_ts", "end_ts", "first_seen", "last_seen",
    "error_count", "warning_count", "current_stage", "current_element",
    "current_section", "current_path", "progress_current", "progress_total",
    "progress_fraction", "compute", "progress_tracks", "pool_tracks",
)

_ADDED_COLUMNS = (
    ("compute", "TEXT"),
    ("progress_tracks", "TEXT"),
    ("current_path", "TEXT"),
    ("pool_tracks", "TEXT"),
)

# Matches UI .lvl checkbox values in static/index.html / app.js logFilterKey().
# "other" covers persisted rows whose topic leaf did not map to a known kind;
# without it those rows are stored but unreachable through every filtered view.
LOG_FILTER_TYPES = frozenset(
    {"error", "warning", "info", "debug", "event", "status", "other"})

# Kinds that the explicit filter keys above already account for. Anything else
# that reaches the events table is reported under "other".
_KNOWN_EVENT_KINDS = ("log", "event", "status")

EVENTS_LIMIT_MAX = 5000
EVENTS_LIMIT_DEFAULT = 2000

RUNS_LIMIT_MAX = 1000
RUNS_LIMIT_DEFAULT = 200


def clamp_events_limit(limit: int) -> int:
    """Clamp a per-request event page size to a safe range."""
    if limit < 1:
        return 1
    if limit > EVENTS_LIMIT_MAX:
        return EVENTS_LIMIT_MAX
    return limit


def clamp_runs_limit(limit: int) -> int:
    """Clamp a run-list page size to a safe range.

    SQLite treats ``LIMIT -1`` as unlimited, so an unvalidated negative limit
    dumped the whole runs table.
    """
    try:
        limit = int(limit)
    except (TypeError, ValueError):
        return RUNS_LIMIT_DEFAULT
    if limit < 1:
        return 1
    if limit > RUNS_LIMIT_MAX:
        return RUNS_LIMIT_MAX
    return limit


def parse_types_param(types: str | list[str] | None) -> list[str] | None:
    """Parse a comma-separated types query into known filter keys, or None for all.

    A request whose keys are *all* unrecognized falls back to "all types". A
    renamed or typo'd UI filter key should degrade to showing too much rather
    than to a silently empty log view.
    """
    if types is None:
        return None
    if isinstance(types, str):
        raw = [part.strip().lower() for part in types.split(",") if part.strip()]
    else:
        raw = [str(part).strip().lower() for part in types if str(part).strip()]
    if not raw:
        return None
    known = [t for t in raw if t in LOG_FILTER_TYPES]
    if not known:
        logger.warning(
            "No recognized event types in %r; returning all types", raw)
        return None
    return known


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
        elif t == "other":
            placeholders = ",".join("?" * len(_KNOWN_EVENT_KINDS))
            parts.append(
                f"(kind IS NULL OR kind NOT IN ({placeholders}))")
            params.extend(_KNOWN_EVENT_KINDS)
    if not parts:
        return "0=1", []
    return "(" + " OR ".join(parts) + ")", params


class DashboardStore:
    """Thread-safe SQLite store for runs and their event streams."""

    # How many inserts between prune passes. Pruning every message forces a
    # DELETE subquery + commit per MQTT log line and dominates under floods.
    _PRUNE_EVERY: int = 1000

    _connection: sqlite3.Connection
    _lock: threading.RLock
    _max_events_per_run: int
    _events_since_prune: dict[str, int]
    _transaction_depth: int

    def __init__(self, database_path: str, max_events_per_run: int = 0) -> None:
        if database_path != ":memory:":
            parent = os.path.dirname(os.path.abspath(database_path))
            os.makedirs(parent, exist_ok=True)

        self._connection = sqlite3.connect(database_path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        # Re-entrant so a caller can hold an explicit transaction() across
        # several store calls, each of which takes the lock again.
        self._lock = threading.RLock()
        self._max_events_per_run = max_events_per_run
        self._events_since_prune = {}
        self._transaction_depth = 0
        self._configure_connection()
        self._initialize_schema()

    def _configure_connection(self) -> None:
        """Apply the write-throughput pragmas.

        At SQLite defaults every commit is an fsync against a rollback journal,
        which is the dominant cost of ingesting a log flood: WAL plus
        ``synchronous=NORMAL`` trades "durable across an OS crash" for "durable
        across a process crash", which is the right trade for a telemetry
        mirror whose source of truth is the build itself.
        """
        with self._lock:
            try:
                self._connection.execute("PRAGMA journal_mode=WAL")
                self._connection.execute("PRAGMA synchronous=NORMAL")
            except sqlite3.DatabaseError as exc:  # pragma: no cover - platform dependent
                logger.warning(
                    "Could not enable WAL/synchronous=NORMAL on %s: %s",
                    getattr(self._connection, "name", "sqlite"), exc)

    def _commit(self) -> None:
        """Commit, unless an enclosing :meth:`transaction` owns the commit."""
        if self._transaction_depth == 0:
            self._connection.commit()

    @contextlib.contextmanager
    def transaction(self) -> Iterator[None]:
        """Batch every store write inside the block into a single commit.

        Also makes read-modify-write sequences (the ``progress_tracks`` blob
        merge in particular) atomic against other writers, since the store lock
        is held for the whole block.
        """
        with self._lock:
            self._transaction_depth += 1
            try:
                yield
            except BaseException:
                self._transaction_depth -= 1
                if self._transaction_depth == 0:
                    self._connection.rollback()
                raise
            self._transaction_depth -= 1
            if self._transaction_depth == 0:
                self._connection.commit()

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

                -- Both sweeps filter on the COALESCE activity expression, so the
                -- indexes have to be on that expression rather than the columns.
                CREATE INDEX IF NOT EXISTS idx_runs_activity
                    ON runs(COALESCE(last_seen, first_seen, 0));
                CREATE INDEX IF NOT EXISTS idx_runs_status_activity
                    ON runs(status, COALESCE(last_seen, first_seen, 0));
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
            self._commit()

    def update_run_fields(self, run_id: str, fields: dict[str, Any]) -> bool:
        """Update the given summary columns for a run, ignoring unknown keys.

        ``None`` means "not supplied, leave the column alone"; pass :data:`CLEAR`
        to write SQL NULL. Returns False when no row matched, which happens when
        a sweeper deleted the run while this projection was in flight.
        """
        updates: dict[str, Any] = {}
        for key, value in fields.items():
            if key not in _RUN_COLUMNS or key == "run_id":
                continue
            if value is None:
                continue
            updates[key] = None if isinstance(value, _ClearSentinel) else value
        if not updates:
            return True

        if "progress_tracks" in updates and not isinstance(updates["progress_tracks"], str):
            updates["progress_tracks"] = json.dumps(updates["progress_tracks"], default=str)
        if "pool_tracks" in updates and not isinstance(updates["pool_tracks"], str):
            updates["pool_tracks"] = json.dumps(updates["pool_tracks"], default=str)

        assignments = ", ".join(f"{column}=?" for column in updates)
        values = list(updates.values())
        values.append(run_id)
        with self._lock:
            cursor = self._connection.execute(
                f"UPDATE runs SET {assignments} WHERE run_id=?", values
            )
            self._commit()
            if cursor.rowcount == 0:
                logger.warning(
                    "Discarded update for unknown run %s (columns: %s)",
                    run_id, ", ".join(sorted(updates)))
                return False
        return True

    def increment_counter(self, run_id: str, column: str) -> bool:
        """Atomically increment ``error_count`` or ``warning_count`` for a run.

        Returns False when no row matched (unknown or already-deleted run).
        """
        if column not in ("error_count", "warning_count"):
            return False
        with self._lock:
            cursor = self._connection.execute(
                f"UPDATE runs SET {column}=COALESCE({column},0)+1 WHERE run_id=?",
                (run_id,),
            )
            self._commit()
            if cursor.rowcount == 0:
                logger.warning(
                    "Discarded %s increment for unknown run %s", column, run_id)
                return False
        return True

    @staticmethod
    def _decode_run_row(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        """Convert a runs row into a JSON-friendly dict with parsed track maps."""
        result = dict(row)
        for key in ("progress_tracks", "pool_tracks"):
            raw = result.get(key)
            if isinstance(raw, str) and raw:
                try:
                    result[key] = json.loads(raw)
                except (TypeError, ValueError):
                    result[key] = {}
            elif raw is None:
                result[key] = {}
        return result

    def list_runs(self, limit: int = 200) -> list[dict[str, Any]]:
        """Return named run summaries for the sidebar.

        Rows without a non-empty ``pipeline`` are omitted so early MQTT stubs
        do not appear as ``(pipeline)`` until early meta arrives.
        """
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM runs
                WHERE COALESCE(TRIM(pipeline), '') != ''
                ORDER BY
                  CASE
                    WHEN COALESCE(status, 'running') IN ('completed', 'failed', 'skipped', 'stopped', 'stale')
                    THEN 1 ELSE 0
                  END ASC,
                  CASE
                    WHEN COALESCE(status, 'running') IN ('completed', 'failed', 'skipped', 'stopped', 'stale')
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

    def get_run_status(self, run_id: str) -> str | None:
        """Return only a run's ``status``, or None when the run is unknown.

        The per-message stale-revival check needs one column; :meth:`get_run`
        would return 23 columns and JSON-decode both track blobs to get it.
        """
        with self._lock:
            row = self._connection.execute(
                "SELECT status FROM runs WHERE run_id=?", (run_id,)
            ).fetchone()
        if row is None:
            return None
        status = row["status"]
        return None if status is None else str(status)

    def get_progress_tracks(self, run_id: str) -> dict[str, Any]:
        """Return the progress_tracks map for a run (empty dict when unset)."""
        run = self.get_run(run_id)
        if run is None:
            return {}
        tracks = run.get("progress_tracks") or {}
        return tracks if isinstance(tracks, dict) else {}

    def get_pool_tracks(self, run_id: str) -> dict[str, Any]:
        """Return the pool_tracks map for a run (empty dict when unset)."""
        run = self.get_run(run_id)
        if run is None:
            return {}
        tracks = run.get("pool_tracks") or {}
        return tracks if isinstance(tracks, dict) else {}

    def clear_run_progress(self, run_id: str) -> None:
        """Clear nested progress/pool tracks and top-level progress columns for a run."""
        if not run_id:
            return
        with self._lock:
            self._connection.execute(
                """
                UPDATE runs SET
                  progress_tracks=?,
                  pool_tracks=?,
                  progress_current=NULL,
                  progress_total=NULL,
                  progress_fraction=NULL
                WHERE run_id=?
                """,
                (json.dumps({}), json.dumps({}), run_id),
            )
            self._commit()

    def delete_run(self, run_id: str) -> bool:
        """Delete a run and its events. Return True when a row was removed."""
        with self._lock:
            cursor = self._connection.execute(
                "DELETE FROM runs WHERE run_id=?", (run_id,)
            )
            self._connection.execute("DELETE FROM events WHERE run_id=?", (run_id,))
            self._commit()
            self._events_since_prune.pop(run_id, None)
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
                        ) -> tuple[list[str], list[str]]:
        """Mark quiet named runs as stale; delete unnamed quiet stubs.

        Non-positive *stale_after_seconds* falls back to 600 so callers cannot
        disable stale detection.

        Returns
        -------
        tuple[list[str], list[str]]
            ``(stale_named_ids, deleted_unnamed_ids)``. Named runs have
            progress cleared when marked stale. Unnamed (null/empty pipeline)
            pulse stubs are deleted instead of listed as ``(pipeline)``.
        """
        # Stale sweep is always-on; non-positive values fall back to the default window.
        if stale_after_seconds <= 0:
            stale_after_seconds = 600.0
        now = time.time() if now is None else now
        cutoff = now - stale_after_seconds
        stale_ids: list[str] = []
        deleted_ids: list[str] = []
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT run_id, pipeline FROM runs
                WHERE status='running' AND COALESCE(last_seen, first_seen, 0) < ?
                """,
                (cutoff,),
            ).fetchall()
            for row in rows:
                run_id = str(row["run_id"])
                pipeline = row["pipeline"]
                named = bool(pipeline and str(pipeline).strip())
                if not named:
                    self._connection.execute(
                        "DELETE FROM runs WHERE run_id=?", (run_id,))
                    self._connection.execute(
                        "DELETE FROM events WHERE run_id=?", (run_id,))
                    self._events_since_prune.pop(run_id, None)
                    deleted_ids.append(run_id)
                    continue
                self._connection.execute(
                    """
                    UPDATE runs SET
                      status=?,
                      progress_tracks=?,
                      pool_tracks=?,
                      progress_current=NULL,
                      progress_total=NULL,
                      progress_fraction=NULL
                    WHERE run_id=?
                    """,
                    ("stale", json.dumps({}), json.dumps({}), run_id),
                )
                stale_ids.append(run_id)
            self._commit()
        return (stale_ids, deleted_ids)

    # -- event helpers ----------------------------------------------------

    def add_event(self, run_id: str, ts: float, kind: str, level: str | None,
                  payload: dict[str, Any]) -> int:
        """Append an event for a run and return its row id.

        Periodically prunes when ``max_events_per_run`` is set so floods do not
        pay for a DELETE on every insert.
        """
        should_prune = False
        with self._lock:
            cursor = self._connection.execute(
                "INSERT INTO events (run_id, ts, kind, level, payload) VALUES (?, ?, ?, ?, ?)",
                (run_id, ts, kind, level, json.dumps(payload, default=str)),
            )
            event_id = int(cursor.lastrowid)
            self._commit()
            if self._max_events_per_run > 0:
                count = self._events_since_prune.get(run_id, 0) + 1
                if count >= self._PRUNE_EVERY:
                    self._events_since_prune[run_id] = 0
                    should_prune = True
                else:
                    self._events_since_prune[run_id] = count
        if should_prune:
            self.prune_events(run_id)
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
        """Trim a run's event history to ``max_events_per_run`` rows.

        Prefer keeping ``log`` error/warning rows so counters stay inspectable;
        fill any remaining budget with the newest other events. A plain
        newest-N prune was discarding early errors under ``iterate_progress``
        floods (including long ``--then`` chains) while ``error_count`` stayed.
        """
        if self._max_events_per_run <= 0:
            return
        limit = self._max_events_per_run
        with self._lock:
            total = int(self._connection.execute(
                "SELECT COUNT(*) FROM events WHERE run_id=?", (run_id,)
            ).fetchone()[0])
            if total <= limit:
                return

            protected_rows = self._connection.execute(
                """
                SELECT id FROM events
                WHERE run_id=?
                  AND kind='log'
                  AND lower(COALESCE(level, '')) IN ('error', 'warning')
                ORDER BY id DESC
                LIMIT ?
                """,
                (run_id, limit),
            ).fetchall()
            keep_ids = {int(row[0]) for row in protected_rows}
            remaining = limit - len(keep_ids)
            if remaining > 0:
                if keep_ids:
                    placeholders = ",".join("?" * len(keep_ids))
                    other_rows = self._connection.execute(
                        f"""
                        SELECT id FROM events
                        WHERE run_id=? AND id NOT IN ({placeholders})
                        ORDER BY id DESC
                        LIMIT ?
                        """,
                        (run_id, *keep_ids, remaining),
                    ).fetchall()
                else:
                    other_rows = self._connection.execute(
                        """
                        SELECT id FROM events
                        WHERE run_id=?
                        ORDER BY id DESC
                        LIMIT ?
                        """,
                        (run_id, remaining),
                    ).fetchall()
                keep_ids.update(int(row[0]) for row in other_rows)

            if not keep_ids:
                return
            placeholders = ",".join("?" * len(keep_ids))
            self._connection.execute(
                f"DELETE FROM events WHERE run_id=? AND id NOT IN ({placeholders})",
                (run_id, *keep_ids),
            )
            self._commit()

    def close(self) -> None:
        """Close the underlying SQLite connection."""
        with self._lock:
            self._connection.close()
