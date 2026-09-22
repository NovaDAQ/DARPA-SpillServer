"""Local SQLite archive of accelerator events.

The TDU keeps only a small ring buffer, so this server's job is to copy events
out of it and hold them long enough to answer historical questions.  SQLite is
a good fit: the write rate is modest (tens of events per second at most), the
read pattern is a range scan over a monotonically increasing key, and the whole
archive stays a single file an operator can copy or back up.

**Deduplication.** Ingest re-reads overlapping windows by design --- a poll
that fetched the last 30 seconds will see events it already has, and a restart
re-reads from the last watermark.  Every insert is therefore idempotent: the
primary key is ``(nova_time, spill_type, signal_code)``, and inserts use
``ON CONFLICT DO NOTHING``.  Re-ingesting the same window is always safe.

``signal_code`` is ``-1`` rather than ``NULL`` when the source could not supply
a raw event word.  SQLite treats ``NULL`` values as distinct in a unique index,
so a nullable column here would silently defeat deduplication.

**Concurrency.** Connections are per-thread (SQLite objects are not shareable
across threads) and the database runs in WAL mode, so the ingest writer never
blocks API readers.  The store itself is synchronous; async callers should
wrap calls in :func:`asyncio.to_thread`.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Sequence

from .signals import SpillType, signal_for_code

__all__ = ["SpillEvent", "QueryResult", "SpillStore", "UNKNOWN_SIGNAL"]

log = logging.getLogger(__name__)

#: Stored in ``signal_code`` when the upstream record carried no raw event
#: word.  A sentinel rather than NULL so the primary key still deduplicates.
UNKNOWN_SIGNAL = -1

_SCHEMA_VERSION = 1

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    nova_time    INTEGER NOT NULL,
    spill_type   INTEGER NOT NULL,
    signal_code  INTEGER NOT NULL DEFAULT -1,
    event_word   INTEGER,
    event_number INTEGER,
    delta        INTEGER,
    pps_offset   INTEGER,
    source       TEXT    NOT NULL DEFAULT 'unknown',
    ingested_at  INTEGER NOT NULL,
    PRIMARY KEY (nova_time, spill_type, signal_code)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_events_time
    ON events (nova_time);
CREATE INDEX IF NOT EXISTS idx_events_type_time
    ON events (spill_type, nova_time);
CREATE INDEX IF NOT EXISTS idx_events_signal_time
    ON events (signal_code, nova_time);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS ingest_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at   INTEGER NOT NULL,
    finished_at  INTEGER,
    source       TEXT,
    fetched      INTEGER DEFAULT 0,
    inserted     INTEGER DEFAULT 0,
    error        TEXT
);
"""


@dataclass
class SpillEvent:
    """One accelerator event as stored in the archive."""

    nova_time: int
    spill_type: SpillType
    signal_code: int = UNKNOWN_SIGNAL
    event_word: Optional[int] = None
    event_number: Optional[int] = None
    delta: Optional[int] = None
    pps_offset: Optional[int] = None
    source: str = "unknown"
    ingested_at: int = field(default_factory=lambda: int(time.time()))

    @property
    def signal_hex(self) -> Optional[str]:
        """Operator-facing signal spelling, or ``None`` if not recorded."""
        if self.signal_code == UNKNOWN_SIGNAL:
            return None
        return "${:02X}".format(self.signal_code)

    @property
    def signal_name(self) -> Optional[str]:
        if self.signal_code == UNKNOWN_SIGNAL:
            return None
        signal = signal_for_code(self.signal_code)
        return signal.name if signal else None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "SpillEvent":
        return cls(
            nova_time=row["nova_time"],
            spill_type=SpillType(row["spill_type"]),
            signal_code=row["signal_code"],
            event_word=row["event_word"],
            event_number=row["event_number"],
            delta=row["delta"],
            pps_offset=row["pps_offset"],
            source=row["source"],
            ingested_at=row["ingested_at"],
        )


@dataclass
class QueryResult:
    """A page of events plus the context a client needs to interpret it."""

    events: List[SpillEvent]
    total: int
    """Total matching rows, ignoring limit and offset."""

    limit: int
    offset: int
    truncated: bool
    """True when *total* exceeds what this page returned."""


class SpillStore:
    """A SQLite-backed archive of accelerator events.

    :param path: database file, or ``":memory:"`` for an ephemeral store.
        Parent directories are created as needed.
    :param timeout: seconds to wait for a write lock before raising.
    """

    def __init__(self, path: str, timeout: float = 15.0) -> None:
        self.path = str(path)
        self.timeout = timeout
        self._local = threading.local()
        # A shared in-memory database would otherwise be a different database
        # in every thread, silently losing every write.
        self._is_memory = self.path == ":memory:"
        self._memory_connection: Optional[sqlite3.Connection] = None
        self._lock = threading.Lock()

        if not self._is_memory:
            Path(self.path).expanduser().parent.mkdir(parents=True, exist_ok=True)
            self.path = str(Path(self.path).expanduser())

        self._initialise()

    # -- connection handling ------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path, timeout=self.timeout, check_same_thread=False
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        if not self._is_memory:
            # WAL lets the ingest writer and API readers proceed together.
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = NORMAL")
        return connection

    @property
    def connection(self) -> sqlite3.Connection:
        """A connection owned by the calling thread."""
        if self._is_memory:
            with self._lock:
                if self._memory_connection is None:
                    self._memory_connection = self._connect()
                return self._memory_connection

        existing = getattr(self._local, "connection", None)
        if existing is None:
            existing = self._connect()
            self._local.connection = existing
        return existing

    def close(self) -> None:
        """Close this thread's connection (and the shared in-memory one)."""
        if self._is_memory:
            with self._lock:
                if self._memory_connection is not None:
                    self._memory_connection.close()
                    self._memory_connection = None
            return
        existing = getattr(self._local, "connection", None)
        if existing is not None:
            existing.close()
            self._local.connection = None

    def _initialise(self) -> None:
        connection = self.connection
        with connection:
            connection.executescript(_SCHEMA)
            connection.execute(
                "INSERT INTO meta (key, value) VALUES ('schema_version', ?) "
                "ON CONFLICT (key) DO NOTHING",
                (str(_SCHEMA_VERSION),),
            )

    # -- metadata -----------------------------------------------------------

    def get_meta(self, key: str, default: Optional[str] = None) -> Optional[str]:
        row = self.connection.execute(
            "SELECT value FROM meta WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row is not None else default

    def set_meta(self, key: str, value: str) -> None:
        with self.connection as connection:
            connection.execute(
                "INSERT INTO meta (key, value) VALUES (?, ?) "
                "ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                (key, str(value)),
            )

    # -- writing ------------------------------------------------------------

    def insert_events(self, events: Iterable[SpillEvent]) -> int:
        """Insert *events*, ignoring any already present.

        Returns the number of rows actually added, which is what the ingest
        loop should log --- the number fetched says nothing about progress
        when windows overlap.
        """
        rows = [
            (
                event.nova_time,
                int(event.spill_type),
                event.signal_code,
                event.event_word,
                event.event_number,
                event.delta,
                event.pps_offset,
                event.source,
                event.ingested_at,
            )
            for event in events
        ]
        if not rows:
            return 0

        connection = self.connection
        with connection:
            before = connection.total_changes
            connection.executemany(
                "INSERT INTO events ("
                "  nova_time, spill_type, signal_code, event_word, "
                "  event_number, delta, pps_offset, source, ingested_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT (nova_time, spill_type, signal_code) DO NOTHING",
                rows,
            )
            return connection.total_changes - before

    # -- reading ------------------------------------------------------------

    def query(
        self,
        start_nova: Optional[int] = None,
        end_nova: Optional[int] = None,
        spill_types: Optional[Sequence[SpillType]] = None,
        signal_codes: Optional[Sequence[int]] = None,
        limit: int = 10_000,
        offset: int = 0,
        descending: bool = False,
    ) -> QueryResult:
        """Return events matching the given filters.

        The range is half-open, ``start_nova <= t < end_nova``, so adjacent
        ranges tile without double-counting an event on the boundary.

        *spill_types* and *signal_codes* combine with OR within themselves and
        AND across each other, matching how the API layer builds them: a
        request for signal ``$8F`` narrows to that signal's code *and* its
        type, while a request naming only a type matches every signal that
        decodes to it.
        """
        where: List[str] = []
        params: List[object] = []

        if start_nova is not None:
            where.append("nova_time >= ?")
            params.append(int(start_nova))
        if end_nova is not None:
            where.append("nova_time < ?")
            params.append(int(end_nova))
        if spill_types:
            placeholders = ", ".join("?" for _ in spill_types)
            where.append("spill_type IN ({})".format(placeholders))
            params.extend(int(t) for t in spill_types)
        if signal_codes:
            placeholders = ", ".join("?" for _ in signal_codes)
            where.append("signal_code IN ({})".format(placeholders))
            params.extend(int(c) for c in signal_codes)

        clause = (" WHERE " + " AND ".join(where)) if where else ""

        total = self.connection.execute(
            "SELECT COUNT(*) AS n FROM events" + clause, params
        ).fetchone()["n"]

        order = "DESC" if descending else "ASC"
        rows = self.connection.execute(
            "SELECT * FROM events{} ORDER BY nova_time {}, spill_type {} "
            "LIMIT ? OFFSET ?".format(clause, order, order),
            (*params, int(limit), int(offset)),
        ).fetchall()

        events = [SpillEvent.from_row(row) for row in rows]
        return QueryResult(
            events=events,
            total=total,
            limit=limit,
            offset=offset,
            truncated=(offset + len(events)) < total,
        )

    def iter_query(
        self,
        start_nova: Optional[int] = None,
        end_nova: Optional[int] = None,
        spill_types: Optional[Sequence[SpillType]] = None,
        signal_codes: Optional[Sequence[int]] = None,
        chunk: int = 5_000,
        descending: bool = False,
    ) -> Iterator[SpillEvent]:
        """Stream every matching event, a page at a time.

        Used by the CSV and JSON exporters so that a query spanning months
        does not have to be materialised in memory before the first byte
        reaches the client.
        """
        offset = 0
        while True:
            page = self.query(
                start_nova=start_nova,
                end_nova=end_nova,
                spill_types=spill_types,
                signal_codes=signal_codes,
                limit=chunk,
                offset=offset,
                descending=descending,
            )
            if not page.events:
                return
            yield from page.events
            offset += len(page.events)
            if offset >= page.total:
                return

    def latest(self, spill_type: Optional[SpillType] = None) -> Optional[SpillEvent]:
        """The most recent stored event, optionally of one type."""
        if spill_type is None:
            row = self.connection.execute(
                "SELECT * FROM events ORDER BY nova_time DESC LIMIT 1"
            ).fetchone()
        else:
            row = self.connection.execute(
                "SELECT * FROM events WHERE spill_type = ? "
                "ORDER BY nova_time DESC LIMIT 1",
                (int(spill_type),),
            ).fetchone()
        return SpillEvent.from_row(row) if row is not None else None

    def earliest(self) -> Optional[SpillEvent]:
        row = self.connection.execute(
            "SELECT * FROM events ORDER BY nova_time ASC LIMIT 1"
        ).fetchone()
        return SpillEvent.from_row(row) if row is not None else None

    def count(self) -> int:
        return self.connection.execute(
            "SELECT COUNT(*) AS n FROM events"
        ).fetchone()["n"]

    def counts_by_type(self) -> "dict[SpillType, int]":
        rows = self.connection.execute(
            "SELECT spill_type, COUNT(*) AS n FROM events GROUP BY spill_type"
        ).fetchall()
        return {SpillType(row["spill_type"]): row["n"] for row in rows}

    # -- housekeeping -------------------------------------------------------

    def prune_before(self, nova_time: int) -> int:
        """Delete events older than *nova_time*; returns rows removed."""
        with self.connection as connection:
            cursor = connection.execute(
                "DELETE FROM events WHERE nova_time < ?", (int(nova_time),)
            )
            return cursor.rowcount

    def vacuum(self) -> None:
        """Reclaim space after a prune.  Blocks writers, so call it rarely."""
        self.connection.execute("VACUUM")

    def record_ingest(
        self,
        source: str,
        started_at: int,
        finished_at: int,
        fetched: int,
        inserted: int,
        error: Optional[str] = None,
    ) -> None:
        with self.connection as connection:
            connection.execute(
                "INSERT INTO ingest_log "
                "(started_at, finished_at, source, fetched, inserted, error) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (started_at, finished_at, source, fetched, inserted, error),
            )

    def recent_ingests(self, limit: int = 20) -> List[sqlite3.Row]:
        return self.connection.execute(
            "SELECT * FROM ingest_log ORDER BY id DESC LIMIT ?", (int(limit),)
        ).fetchall()

    def trim_ingest_log(self, keep: int = 1000) -> int:
        """Keep only the most recent *keep* ingest records."""
        with self.connection as connection:
            cursor = connection.execute(
                "DELETE FROM ingest_log WHERE id NOT IN "
                "(SELECT id FROM ingest_log ORDER BY id DESC LIMIT ?)",
                (int(keep),),
            )
            return cursor.rowcount
