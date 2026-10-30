"""!
@file src/darpa_spillserver/storage.py
@brief SQLite archive of accelerator events (schema, migration, queries).

Local SQLite archive of accelerator events.

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

## Stored in ``signal_code`` when the upstream record carried no raw event
## word.  A sentinel rather than NULL so the primary key still deduplicates.
UNKNOWN_SIGNAL = -1

## Schema version written to ``meta.schema_version``.  2 added per-TDU
## ``source`` (with the old route moved to ``route``); see _migrate_v1.
_SCHEMA_VERSION = 2

## Stored as the source of rows migrated from a version 1 archive when the
## caller cannot say which TDU they came from.
UNKNOWN_SOURCE = "unknown"

## DDL of the events table.  nova_time is the 64 MHz tick count (fits
## SQLite's 64-bit INTEGER for ~4500 years).  WITHOUT ROWID clusters rows on
## the primary key, whose leading column is nova_time, so a time-range scan
## reads contiguous pages.  ingested_at is UNIX seconds (host clock).
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

## Secondary indexes, each ending in nova_time so every filtered query is
## still a range scan in time order.
_EVENT_INDEXES = (
    "CREATE INDEX idx_events_time ON events (nova_time)",
    "CREATE INDEX idx_events_type_time ON events (spill_type, nova_time)",
    "CREATE INDEX idx_events_signal_time ON events (signal_code, nova_time)",
    "CREATE INDEX idx_events_source_time ON events (source, nova_time)",
)

## Auxiliary tables, all CREATE IF NOT EXISTS so they can be added within a
## schema version without a migration.
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
    """!
    @brief The archive cannot be opened as this version expects.
    """


@dataclass
class SpillEvent:
    """!
    @brief One accelerator event as stored in the archive.
    """

    ## Event time, 64 MHz NOvA ticks since 2010-01-01T00:00:00Z.
    nova_time: int
    ## Decoded type (wire value of the C++ enum).
    spill_type: SpillType
    ## Low byte of the raw event word, or UNKNOWN_SIGNAL.
    signal_code: int = UNKNOWN_SIGNAL
    ## Full 16-bit event word, when the source supplied it.
    event_word: Optional[int] = None
    ## The TDU's own event counter; increases in true time order per TDU.
    event_number: Optional[int] = None
    ## Ticks since the previous event of the same TDU, as the TDU reports it.
    delta: Optional[int] = None
    ## Ticks since the last whole second (the sub-second phase), per the TDU.
    pps_offset: Optional[int] = None
    ## Name of the TDU the event was read from.
    source: str = UNKNOWN_SOURCE
    """Name of the TDU the event was read from."""

    ## TDU route that supplied the event.
    route: str = ""
    """TDU route that supplied it, e.g. ``spill_history``."""

    ## UNIX seconds (host clock) at ingest; set when the object is built for
    ## insert, read back from the row otherwise.
    ingested_at: int = field(default_factory=lambda: int(time.time()))

    @property
    def signal_hex(self) -> Optional[str]:
        """!
        @brief Operator-facing signal spelling, or ``None`` if not recorded.

        @return ``$XX`` or None.
        """
        if self.signal_code == UNKNOWN_SIGNAL:
            return None
        return "${:02X}".format(self.signal_code)

    @property
    def signal_name(self) -> Optional[str]:
        """!
        @brief Registry name of the signal, or None if not recorded or not
               registered.

        @return Signal name or None.
        """
        if self.signal_code == UNKNOWN_SIGNAL:
            return None
        signal = signal_for_code(self.signal_code)
        return signal.name if signal else None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "SpillEvent":
        """!
        @brief Build an event from a ``SELECT *`` row of the events table.

        @param row sqlite3.Row with every events column.
        @return The event.
        @throws ValueError if spill_type is not a known SpillType value.
        """
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
    """!
    @brief A page of events plus the context a client needs to interpret it.
    """

    ## The rows of this page, in the requested order.
    events: List[SpillEvent]
    ## Total matching rows, ignoring limit and offset.
    total: int
    """Total matching rows, ignoring limit and offset."""

    ## Page size requested.
    limit: int
    ## Rows skipped before this page.
    offset: int
    ## True when rows exist beyond this page.
    truncated: bool
    """True when *total* exceeds what this page returned."""


class SpillStore:
    """!
    @brief A SQLite-backed archive of accelerator events.

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
        """!
        @brief Open (creating or migrating as needed) the archive at *path*.

        @param path Database file, or ``":memory:"``.
        @param timeout Seconds to wait for a lock (sqlite3 busy timeout).
        @param legacy_source Source name for rows of a migrated v1 archive.
        @throws StoreError if the archive's schema is newer than this code.
        @throws sqlite3.Error if the file is not a usable database.
        """
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
        """!
        @brief Open a new connection with the archive's pragmas.

        @details check_same_thread=False is needed only for the shared
        in-memory connection; file connections stay per-thread by
        construction (see the connection property).
        @return A configured sqlite3 connection with Row factory.
        """
        connection = sqlite3.connect(
            self.path, timeout=self.timeout, check_same_thread=False
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        if not self._is_memory:
            # WAL lets the ingest writer and API readers proceed together.
            connection.execute("PRAGMA journal_mode = WAL")
            # NORMAL in WAL mode is durable against a process crash; only an
            # OS crash/power loss can lose the last commits, which a re-poll
            # of the TDU ring buffer or a backfill recovers.
            connection.execute("PRAGMA synchronous = NORMAL")
        return connection

    @property
    def connection(self) -> sqlite3.Connection:
        """!
        @brief A connection owned by the calling thread.

        @details For ``:memory:`` every thread shares one connection, since
        each new in-memory connection is a separate empty database.
        @return The connection, created on first use.
        """
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
        """!
        @brief Close this thread's connection (and the shared in-memory one).

        @details Connections opened by other threads are not closed; they
        are released when those threads' thread-local storage is collected.
        """
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
        """!
        @brief Create a new schema, migrate a v1 archive, or just add any
               missing auxiliary tables to a current one.

        @throws StoreError if the archive's schema is newer than this code.
        """
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
        # IMMEDIATE takes the write lock before any DDL runs.  Note that
        # *version* was read before the lock was taken.
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
        """!
        @brief The archive's schema version, or ``None`` for a new database.

        @details An events table with no recorded version is a version 1
        archive, which predates the meta.schema_version key.
        @param connection Open connection.
        @return Schema version, or None if there is no events table.
        """
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
        """!
        @brief Rebuild a version 1 archive with per-source keys.

        The primary key changes, which SQLite can only do by copying the
        table.  The old ``source`` column held the route, so it moves to
        ``route``, and every row is tagged with :attr:`legacy_source`.

        @details Runs inside the caller's open transaction; nothing here
        commits.
        @param connection Connection holding the migration transaction.
        """
        name = self.legacy_source or UNKNOWN_SOURCE
        connection.execute("ALTER TABLE events RENAME TO events_v1")
        # The renamed table keeps its indexes under their old names, which
        # would collide with _EVENT_INDEXES; v1 had only these three.
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
        # Same route/source split for the ingest log; skipped if a partial
        # earlier run or a newer ingest_log DDL already has the column.
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
        """!
        @brief Read a value from the meta key/value table.

        @param key Key name.
        @param default Returned when the key is absent.
        @return The stored text, or *default*.
        """
        row = self.connection.execute(
            "SELECT value FROM meta WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row is not None else default

    def set_meta(self, key: str, value: str) -> None:
        """!
        @brief Insert or replace a value in the meta table.

        @param key Key name.
        @param value Value; stored as str(value).
        """
        with self.connection as connection:
            connection.execute(
                "INSERT INTO meta (key, value) VALUES (?, ?) "
                "ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                (key, str(value)),
            )

    # -- writing ------------------------------------------------------------

    def insert_events(self, events: Iterable[SpillEvent]) -> int:
        """!
        @brief Insert *events*, ignoring any already present.

        Returns the number of rows actually added, which is what the ingest
        loop should log --- the number fetched says nothing about progress
        when windows overlap.

        @param events Events to insert (materialised into a list first).
        @return Number of rows actually inserted.
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
            # total_changes counts only rows actually inserted, so the delta
            # excludes duplicates skipped by ON CONFLICT DO NOTHING.
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
        unrestricted_types: Optional[Sequence[SpillType]] = None,
        count_total: bool = True,
    ) -> QueryResult:
        """!
        @brief Return events matching the given filters.

        The range is half-open, ``start_nova <= t < end_nova``, so adjacent
        ranges tile without double-counting an event on the boundary.

        *spill_types* and *signal_codes* combine with OR within themselves and
        AND across each other, matching how the API layer builds them: a
        request for signal ``$8F`` narrows to that signal's code *and* its
        type, while a request naming only a type matches every signal that
        decodes to it.

        *unrestricted_types* bypasses the raw-code predicate for explicitly
        selected types. With *count_total=False*, total is only the observed
        offset plus returned rows, and truncated is false; use this for latest
        lookups that do not expose pagination metadata.

        *sources* restricts to events from those TDUs; ``None`` means all.

        @param start_nova Inclusive lower bound, NOvA ticks; None is unbounded.
        @param end_nova Exclusive upper bound, NOvA ticks; None is unbounded.
        @param spill_types Types to match (OR); empty/None means any.
        @param signal_codes Codes to match (OR); empty/None means any.
        @param limit Maximum rows to return.
        @param offset Rows to skip.
        @param descending Newest first instead of oldest first.
        @param sources TDU names to match (OR); empty/None means all.
        @return The page and the total match count.
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
            predicate = "signal_code IN ({})".format(placeholders)
            params.extend(int(c) for c in signal_codes)
            if unrestricted_types:
                predicate += " OR spill_type IN ({})".format(", ".join("?" for _ in unrestricted_types))
                params.extend(int(t) for t in unrestricted_types)
            where.append("(" + predicate + ")")
        if sources:
            placeholders = ", ".join("?" for _ in sources)
            where.append("source IN ({})".format(placeholders))
            params.extend(str(name) for name in sources)

        # Only placeholders are interpolated into the SQL text; every value
        # is bound as a parameter.
        clause = (" WHERE " + " AND ".join(where)) if where else ""

        # Two statements, not one snapshot: a concurrent insert between them
        # can make total and the page disagree by a few rows.
        total = self.connection.execute(
            "SELECT COUNT(*) AS n FROM events" + clause, params
        ).fetchone()["n"] if count_total else None

        # The tie-break columns make the order deterministic for events that
        # share a timestamp (several TDUs see the same accelerator event).
        # signal_code, the remaining primary-key column, is not included.
        order = "DESC" if descending else "ASC"
        rows = self.connection.execute(
            "SELECT * FROM events{} ORDER BY nova_time {}, spill_type {}, "
            "source {} LIMIT ? OFFSET ?".format(clause, order, order, order),
            (*params, int(limit), int(offset)),
        ).fetchall()

        events = [SpillEvent.from_row(row) for row in rows]
        if total is None:
            total = offset + len(events)
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
        unrestricted_types: Optional[Sequence[SpillType]] = None,
    ) -> Iterator[SpillEvent]:
        """!
        @brief Stream every matching event, a page at a time.

        Used by the CSV and JSON exporters so that a query spanning months
        does not have to be materialised in memory before the first byte
        reaches the client.

        @details Pages by LIMIT/OFFSET, so cost grows with the square of the
        row count and a concurrent insert inside the range can shift a page
        boundary; darpa-spill-dump avoids this by walking time windows.
        @param start_nova Inclusive lower bound, NOvA ticks.
        @param end_nova Exclusive upper bound, NOvA ticks.
        @param spill_types Types to match; empty/None means any.
        @param signal_codes Codes to match; empty/None means any.
        @param chunk Rows per underlying query.
        @param descending Newest first.
        @param sources TDU names to match; empty/None means all.
        @return Generator of events.
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
                unrestricted_types=unrestricted_types,
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
        before: Optional[int] = None,
    ) -> Optional[SpillEvent]:
        """!
        @brief The most recent stored event, optionally of one type or source,
        and optionally only among events earlier than *before* (NOvA ticks).

        @param spill_type Restrict to this type; None means any.
        @param sources Restrict to these TDUs; empty/None means all.
        @param before Exclusive upper bound, NOvA ticks; None is unbounded.
        @return The event, or None if nothing matches.
        """
        return self._extreme("DESC", spill_type, sources, before)

    def earliest(
        self, sources: Optional[Sequence[str]] = None
    ) -> Optional[SpillEvent]:
        """!
        @brief The oldest stored event, optionally from given sources.

        @param sources Restrict to these TDUs; empty/None means all.
        @return The event, or None for an empty selection.
        """
        return self._extreme("ASC", None, sources)

    def _extreme(self, order, spill_type, sources,
                 before: Optional[int] = None) -> Optional[SpillEvent]:
        """!
        @brief First row by nova_time in the given direction.

        @param order ``"ASC"`` (earliest) or ``"DESC"`` (latest); trusted,
                     interpolated into the SQL.
        @param spill_type Restrict to this type; None means any.
        @param sources Restrict to these TDUs; empty/None means all.
        @param before Exclusive upper bound, NOvA ticks; None is unbounded.
        @return The event, or None.
        """
        where: List[str] = []
        params: List[object] = []
        if before is not None:
            where.append("nova_time < ?")
            params.append(int(before))
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
        """!
        @brief Total rows in the archive.

        @details A full scan; slow on a large archive.
        @return Row count.
        """
        return self.connection.execute(
            "SELECT COUNT(*) AS n FROM events"
        ).fetchone()["n"]

    def counts_by_source(self) -> "dict[str, int]":
        """!
        @brief Row count per source.

        @details A full scan; slow on a large archive.
        @return Mapping source name -> count, sorted by name.
        """
        rows = self.connection.execute(
            "SELECT source, COUNT(*) AS n FROM events GROUP BY source "
            "ORDER BY source"
        ).fetchall()
        return {row["source"]: row["n"] for row in rows}

    def source_names(self) -> List[str]:
        """!
        @brief Every source that has at least one event in the archive.

        @return Source names in ascending order.
        """
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
        """!
        @brief Row count per spill type.

        @details A full scan; slow on a large archive.
        @return Mapping SpillType -> count.
        """
        rows = self.connection.execute(
            "SELECT spill_type, COUNT(*) AS n FROM events GROUP BY spill_type"
        ).fetchall()
        return {SpillType(row["spill_type"]): row["n"] for row in rows}

    # -- housekeeping -------------------------------------------------------

    def prune_before(self, nova_time: int, source: Optional[str] = None) -> int:
        """!
        @brief Delete events older than *nova_time*, optionally from one source.

        Returns the number of rows removed.

        @param nova_time Exclusive cut, NOvA ticks.
        @param source Restrict to this TDU; None means all.
        @return Number of rows deleted.
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
        """!
        @brief Reclaim space after a prune.  Blocks writers, so call it rarely.
        """
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
        """!
        @brief Append one poll outcome to the ingest log.

        @param source TDU name.
        @param route TDU route polled.
        @param started_at Poll start, UNIX seconds.
        @param finished_at Poll end, UNIX seconds.
        @param fetched Records returned by the TDU.
        @param inserted Rows actually added (after deduplication).
        @param error Error text, or None on success.
        """
        with self.connection as connection:
            connection.execute(
                "INSERT INTO ingest_log "
                "(started_at, finished_at, source, route, fetched, inserted, "
                " error) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (started_at, finished_at, source, route, fetched, inserted,
                 error),
            )

    def recent_ingests(self, limit: int = 20) -> List[sqlite3.Row]:
        """!
        @brief The newest ingest-log rows.

        @param limit Maximum rows.
        @return Rows, newest first.
        """
        return self.connection.execute(
            "SELECT * FROM ingest_log ORDER BY id DESC LIMIT ?", (int(limit),)
        ).fetchall()

    def trim_ingest_log(self, keep: int = 1000) -> int:
        """!
        @brief Keep only the most recent *keep* ingest records.

        @param keep Rows to retain (by id, i.e. insertion order).
        @return Number of rows deleted.
        """
        with self.connection as connection:
            cursor = connection.execute(
                "DELETE FROM ingest_log WHERE id NOT IN "
                "(SELECT id FROM ingest_log ORDER BY id DESC LIMIT ?)",
                (int(keep),),
            )
            return cursor.rowcount

    # -- runtime source overrides --------------------------------------------

    def source_overrides(self) -> "dict[str, sqlite3.Row]":
        """!
        @brief Changes made to sources at runtime, keyed by source name.

        @return Mapping name -> row (name, base_url, enabled, updated_at,
                updated_by).
        """
        rows = self.connection.execute(
            "SELECT * FROM source_overrides ORDER BY name"
        ).fetchall()
        return {row["name"]: row for row in rows}

    def set_source_override(
        self, name: str, base_url: str, enabled: bool, updated_by: str = ""
    ) -> None:
        """!
        @brief Insert or replace the runtime override for one source.

        @param name Source name.
        @param base_url TDU base URL to use.
        @param enabled Whether the source is polled.
        @param updated_by Identity of the user making the change.
        """
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
        """!
        @brief Drop a runtime override; returns whether there was one.

        @param name Source name.
        @return True if an override was deleted.
        """
        with self.connection as connection:
            cursor = connection.execute(
                "DELETE FROM source_overrides WHERE name = ?", (name,)
            )
            return cursor.rowcount > 0
