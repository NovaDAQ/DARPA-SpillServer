"""Local SQLite archive of accelerator events.

The TDU keeps only a small ring buffer, so this server's job is to copy events
out of it and hold them long enough to answer historical questions.  SQLite is
a good fit: the write rate is modest (tens of events per second at most), the
read pattern is a range scan over a monotonically increasing key, and the whole
archive stays a single file an operator can copy or back up.

**Sources.** One archive holds events from several TDUs.  Each row carries
``source``, the name of the TDU it came from, and ``route``, the TDU route
that supplied it (``spill_history`` or ``tcr_status``).  Two TDUs that see
the same accelerator event store it twice, once each, because they are two
independent measurements of it.

**Deduplication.** Ingest re-reads overlapping windows by design --- a poll
that fetched the last 30 seconds will see events it already has, and a restart
re-reads from the last watermark.  Every insert is therefore idempotent: the
primary key is ``(nova_time, spill_type, signal_code, source)``, and inserts
use ``ON CONFLICT DO NOTHING``.  Re-ingesting the same window is always safe.

``signal_code`` is ``-1`` rather than ``NULL`` when the source could not supply
a raw event word.  SQLite treats ``NULL`` values as distinct in a unique index,
so a nullable column here would silently defeat deduplication.

**Concurrency.** Connections are per-thread (SQLite objects are not shareable
across threads) and the database runs in WAL mode, so the ingest writer never
blocks API readers.  The store itself is synchronous; async callers should
wrap calls in :func:`asyncio.to_thread`.

**Schema versions.** Version 1 archives predate sources: their ``source``
column held the route, and there was one TDU.  Opening one migrates it in
place, in a single transaction, tagging every existing row with the
*legacy_source* name the caller supplies --- the TDU those events came from.
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

__all__ = ["SpillEvent", "QueryResult", "SpillStore", "StoreError", "UNKNOWN_SOURCE",
           "UNKNOWN_SIGNAL"]

log = logging.getLogger(__name__)

#: Stored in ``signal_code`` when the upstream record carried no raw event
#: word.  A sentinel rather than NULL so the primary key still deduplicates.
UNKNOWN_SIGNAL = -1

_SCHEMA_VERSION = 2

#: Stored as the source of rows migrated from a version 1 archive when the
#: caller cannot say which TDU they came from.
UNKNOWN_SOURCE = "unknown"

_EVENTS_TABLE = """
CREATE TABLE events (
    nova_time    INTEGER NOT NULL,
    spill_type   INTEGER NOT NULL,
    signal_code  INTEGER NOT NULL DEFAULT -1,
    event_word   INTEGER,
    event_number INTEGER,
    delta        INTEGER,
    pps_offset   INTEGER,
    source       TEXT    NOT NULL,
    route        TEXT    NOT NULL DEFAULT '',
    ingested_at  INTEGER NOT NULL,
    PRIMARY KEY (nova_time, spill_type, signal_code, source)
) WITHOUT ROWID
"""

_EVENT_INDEXES = (
    "CREATE INDEX idx_events_time ON events (nova_time)",
    "CREATE INDEX idx_events_type_time ON events (spill_type, nova_time)",
    "CREATE INDEX idx_events_signal_time ON events (signal_code, nova_time)",
    "CREATE INDEX idx_events_source_time ON events (source, nova_time)",
)

_OTHER_TABLES = (
    """CREATE TABLE IF NOT EXISTS meta (
        key   TEXT PRIMARY KEY,
        value TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS ingest_log (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        started_at   INTEGER NOT NULL,
        finished_at  INTEGER,
        source       TEXT,
        route        TEXT,
        fetched      INTEGER DEFAULT 0,
        inserted     INTEGER DEFAULT 0,
        error        TEXT
    )""",
    # Changes made at runtime from the /config page. They outlive a restart,
    # and win over the configuration file until reset.
    """CREATE TABLE IF NOT EXISTS source_overrides (
        name        TEXT PRIMARY KEY,
        base_url    TEXT    NOT NULL,
        enabled     INTEGER NOT NULL,
        updated_at  INTEGER NOT NULL,
        updated_by  TEXT    NOT NULL DEFAULT ''
    )""",
)


class StoreError(RuntimeError):
    """The archive cannot be opened as this version expects."""


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
    source: str = UNKNOWN_SOURCE
    """Name of the TDU the event was read from."""

    route: str = ""
    """TDU route that supplied it, e.g. ``spill_history``."""

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
            route=row["route"],
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
    :param legacy_source: source name given to the rows of a version 1
        archive when it is migrated.  Those archives recorded one TDU, so
        this is normally the first configured source.
    """

    def __init__(
        self,
        path: str,
        timeout: float = 15.0,
        legacy_source: Optional[str] = None,
    ) -> None:
        self.path = str(path)
        self.timeout = timeout
        self.legacy_source = legacy_source
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
        version = self._existing_version(connection)
        if version == _SCHEMA_VERSION:
            # Tables added within a version are created IF NOT EXISTS.
            with connection:
                for statement in _OTHER_TABLES:
                    connection.execute(statement)
            return
        if version is not None and version > _SCHEMA_VERSION:
            raise StoreError(
                "{} has schema version {}, newer than this server's {}; "
                "upgrade darpa-spillserver to read it".format(
                    self.path, version, _SCHEMA_VERSION
                )
            )

        # DDL is not transactional under the sqlite3 module's default
        # handling, so the transaction is opened by hand: a migration that
        # fails halfway must leave the version 1 archive exactly as it was.
        connection.execute("BEGIN IMMEDIATE")
        try:
            for statement in _OTHER_TABLES:
                connection.execute(statement)
            if version is None:
                connection.execute(_EVENTS_TABLE)
                for statement in _EVENT_INDEXES:
                    connection.execute(statement)
            else:
                self._migrate_v1(connection)
            connection.execute(
                "INSERT INTO meta (key, value) VALUES ('schema_version', ?) "
                "ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                (str(_SCHEMA_VERSION),),
            )
        except BaseException:
            connection.rollback()
            raise
        connection.commit()

    @staticmethod
    def _existing_version(connection: sqlite3.Connection) -> Optional[int]:
        """The archive's schema version, or ``None`` for a new database."""
        tables = {
            row["name"] for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        if "events" not in tables:
            return None
        if "meta" in tables:
            row = connection.execute(
                "SELECT value FROM meta WHERE key = 'schema_version'"
            ).fetchone()
            if row is not None:
                return int(row["value"])
        return 1

    def _migrate_v1(self, connection: sqlite3.Connection) -> None:
        """Rebuild a version 1 archive with per-source keys.

        The primary key changes, which SQLite can only do by copying the
        table.  The old ``source`` column held the route, so it moves to
        ``route``, and every row is tagged with :attr:`legacy_source`.
        """
        name = self.legacy_source or UNKNOWN_SOURCE
        connection.execute("ALTER TABLE events RENAME TO events_v1")
        for index in ("idx_events_time", "idx_events_type_time",
                      "idx_events_signal_time"):
            connection.execute("DROP INDEX IF EXISTS {}".format(index))
        connection.execute(_EVENTS_TABLE)
        cursor = connection.execute(
            "INSERT INTO events (nova_time, spill_type, signal_code, "
            "  event_word, event_number, delta, pps_offset, source, route, "
            "  ingested_at) "
            "SELECT nova_time, spill_type, signal_code, event_word, "
            "  event_number, delta, pps_offset, ?, source, ingested_at "
            "FROM events_v1",
            (name,),
        )
        migrated = cursor.rowcount
        connection.execute("DROP TABLE events_v1")
        for statement in _EVENT_INDEXES:
            connection.execute(statement)

        columns = {
            row["name"] for row in connection.execute("PRAGMA table_info(ingest_log)")
        }
        if "route" not in columns:
            connection.execute("ALTER TABLE ingest_log ADD COLUMN route TEXT")
            connection.execute(
                "UPDATE ingest_log SET route = source, source = ?", (name,)
            )
        log.warning(
            "migrated %s to schema version %d: tagged %d existing event(s) "
            "with source %r",
            self.path, _SCHEMA_VERSION, migrated, name,
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
                event.route,
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
                "  event_number, delta, pps_offset, source, route, ingested_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT (nova_time, spill_type, signal_code, source) "
                "DO NOTHING",
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
        sources: Optional[Sequence[str]] = None,
    ) -> QueryResult:
        """Return events matching the given filters.

        The range is half-open, ``start_nova <= t < end_nova``, so adjacent
        ranges tile without double-counting an event on the boundary.

        *spill_types* and *signal_codes* combine with OR within themselves and
        AND across each other, matching how the API layer builds them: a
        request for signal ``$8F`` narrows to that signal's code *and* its
        type, while a request naming only a type matches every signal that
        decodes to it.

        *sources* restricts to events from those TDUs; ``None`` means all.
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
        if sources:
            placeholders = ", ".join("?" for _ in sources)
            where.append("source IN ({})".format(placeholders))
            params.extend(str(name) for name in sources)

        clause = (" WHERE " + " AND ".join(where)) if where else ""

        total = self.connection.execute(
            "SELECT COUNT(*) AS n FROM events" + clause, params
        ).fetchone()["n"]

        order = "DESC" if descending else "ASC"
        rows = self.connection.execute(
            "SELECT * FROM events{} ORDER BY nova_time {}, spill_type {}, "
            "source {} LIMIT ? OFFSET ?".format(clause, order, order, order),
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
        sources: Optional[Sequence[str]] = None,
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
                sources=sources,
            )
            if not page.events:
                return
            yield from page.events
            offset += len(page.events)
            if offset >= page.total:
                return

    def latest(
        self,
        spill_type: Optional[SpillType] = None,
        sources: Optional[Sequence[str]] = None,
    ) -> Optional[SpillEvent]:
        """The most recent stored event, optionally of one type or source."""
        return self._extreme("DESC", spill_type, sources)

    def earliest(
        self, sources: Optional[Sequence[str]] = None
    ) -> Optional[SpillEvent]:
        return self._extreme("ASC", None, sources)

    def _extreme(self, order, spill_type, sources) -> Optional[SpillEvent]:
        where: List[str] = []
        params: List[object] = []
        if spill_type is not None:
            where.append("spill_type = ?")
            params.append(int(spill_type))
        if sources:
            where.append("source IN ({})".format(", ".join("?" for _ in sources)))
            params.extend(sources)
        clause = (" WHERE " + " AND ".join(where)) if where else ""
        row = self.connection.execute(
            "SELECT * FROM events{} ORDER BY nova_time {} LIMIT 1".format(
                clause, order),
            params,
        ).fetchone()
        return SpillEvent.from_row(row) if row is not None else None

    def count(self) -> int:
        return self.connection.execute(
            "SELECT COUNT(*) AS n FROM events"
        ).fetchone()["n"]

    def counts_by_source(self) -> "dict[str, int]":
        rows = self.connection.execute(
            "SELECT source, COUNT(*) AS n FROM events GROUP BY source "
            "ORDER BY source"
        ).fetchall()
        return {row["source"]: row["n"] for row in rows}

    def source_names(self) -> List[str]:
        """Every source that has at least one event in the archive."""
        # A loose index scan: one probe per distinct source instead of a
        # full pass over the index, which matters on a years-long archive.
        names: List[str] = []
        row = self.connection.execute(
            "SELECT MIN(source) AS name FROM events"
        ).fetchone()
        while row is not None and row["name"] is not None:
            names.append(row["name"])
            row = self.connection.execute(
                "SELECT MIN(source) AS name FROM events WHERE source > ?",
                (row["name"],),
            ).fetchone()
        return names

    def counts_by_type(self) -> "dict[SpillType, int]":
        rows = self.connection.execute(
            "SELECT spill_type, COUNT(*) AS n FROM events GROUP BY spill_type"
        ).fetchall()
        return {SpillType(row["spill_type"]): row["n"] for row in rows}

    # -- housekeeping -------------------------------------------------------

    def prune_before(self, nova_time: int, source: Optional[str] = None) -> int:
        """Delete events older than *nova_time*, optionally from one source.

        Returns the number of rows removed.
        """
        with self.connection as connection:
            if source is None:
                cursor = connection.execute(
                    "DELETE FROM events WHERE nova_time < ?", (int(nova_time),)
                )
            else:
                cursor = connection.execute(
                    "DELETE FROM events WHERE source = ? AND nova_time < ?",
                    (source, int(nova_time)),
                )
            return cursor.rowcount

    def vacuum(self) -> None:
        """Reclaim space after a prune.  Blocks writers, so call it rarely."""
        self.connection.execute("VACUUM")

    def record_ingest(
        self,
        source: str,
        route: str,
        started_at: int,
        finished_at: int,
        fetched: int,
        inserted: int,
        error: Optional[str] = None,
    ) -> None:
        with self.connection as connection:
            connection.execute(
                "INSERT INTO ingest_log "
                "(started_at, finished_at, source, route, fetched, inserted, "
                " error) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (started_at, finished_at, source, route, fetched, inserted,
                 error),
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

    # -- runtime source overrides --------------------------------------------

    def source_overrides(self) -> "dict[str, sqlite3.Row]":
        """Changes made to sources at runtime, keyed by source name."""
        rows = self.connection.execute(
            "SELECT * FROM source_overrides ORDER BY name"
        ).fetchall()
        return {row["name"]: row for row in rows}

    def set_source_override(
        self, name: str, base_url: str, enabled: bool, updated_by: str = ""
    ) -> None:
        with self.connection as connection:
            connection.execute(
                "INSERT INTO source_overrides "
                "(name, base_url, enabled, updated_at, updated_by) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT (name) DO UPDATE SET base_url = excluded.base_url, "
                "enabled = excluded.enabled, updated_at = excluded.updated_at, "
                "updated_by = excluded.updated_by",
                (name, base_url, int(bool(enabled)), int(time.time()),
                 updated_by),
            )

    def clear_source_override(self, name: str) -> bool:
        """Drop a runtime override; returns whether there was one."""
        with self.connection as connection:
            cursor = connection.execute(
                "DELETE FROM source_overrides WHERE name = ?", (name,)
            )
            return cursor.rowcount > 0
