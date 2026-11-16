"""
dlq_store.py - the shared core of the dead letter queue.

Used by consumer.py (which dead-letters bad messages) and dlq.py (which lets a
person inspect, replay and discard them).

Life of a dead-lettered message (the `status` column):

    pending    failed, waiting for someone to look at it
    replayed   re-published to the original topic, waiting to see what happens
    resolved   a replay succeeded
    discarded  a person decided it is not worth fixing (never replayed again)
    poison     failed MAX_ATTEMPTS times in total; never replayed automatically

Two rules keep this from looping forever:
  * a replayed message carries its DLQ id and attempt number in Kafka headers,
    so when it fails again the consumer UPDATES the existing record
    (attempt_count + 1) instead of creating a new one;
  * once attempt_count reaches MAX_ATTEMPTS the record becomes `poison`.
"""

import base64
import re
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import RealDictCursor

DLQ_TOPIC = "sentences.dlq"
MAX_ATTEMPTS = 3  # total failures (the original + replays) before a message is poison
HEADER_ID = "dlq-id"
HEADER_ATTEMPT = "dlq-attempt"
STATUSES = ("pending", "replayed", "resolved", "discarded", "poison")

DDL = """
CREATE TABLE IF NOT EXISTS dlq_records (
    id               BIGSERIAL PRIMARY KEY,
    source_topic     TEXT NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset    BIGINT NOT NULL,
    original_message TEXT NOT NULL,        -- readable copy (undecodable bytes show as \\xNN)
    original_payload BYTEA NOT NULL,       -- the exact bytes, used for replay
    error_type       TEXT NOT NULL,
    error_message    TEXT NOT NULL,
    consumer         TEXT NOT NULL,
    instance         INTEGER,
    failed_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    first_failed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempt_count    INTEGER NOT NULL DEFAULT 1,
    status           TEXT NOT NULL DEFAULT 'pending',
    replay_count     INTEGER NOT NULL DEFAULT 0,
    last_replayed_at TIMESTAMPTZ,
    resolved_at      TIMESTAMPTZ,
    UNIQUE (source_topic, source_partition, source_offset)
);
CREATE INDEX IF NOT EXISTS dlq_records_status_idx ON dlq_records (status, failed_at);
"""

COLUMNS = (
    "id, source_topic, source_partition, source_offset, original_message, original_payload, "
    "error_type, error_message, consumer, instance, failed_at, first_failed_at, attempt_count, "
    "status, replay_count, last_replayed_at, resolved_at"
)

_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


class DlqError(RuntimeError):
    """A DLQ operation that can't be done (unknown id, already discarded, ...)."""


# ---------------------------------------------------------------- small helpers

def parse_duration(text):
    """'30s', '15m', '1h', '2d' -> seconds."""
    m = re.fullmatch(r"\s*(\d+)\s*([smhd])\s*", str(text).lower())
    if not m:
        raise DlqError(f"Bad duration '{text}'. Use a number and a unit, like 30s, 15m, 1h or 2d.")
    return int(m.group(1)) * _UNITS[m.group(2)]


def iso(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def readable(payload):
    """Text for humans. Bytes that aren't valid UTF-8 show as \\xNN instead of failing."""
    text = payload.decode("utf-8", errors="backslashreplace")
    return text.replace("\x00", "\\x00")  # Postgres text can't hold NUL


def classify(exc):
    """Is this failure the message's fault, or the system's?

    permanent: something about THIS message (bad JSON, bad bytes, missing field).
               Retrying it unchanged fails the same way, so it belongs in the DLQ.
    transient: something about the SYSTEM (database down, network drop, Kafka
               unreachable). The message is fine and will work later, so it must
               NOT go to the DLQ; the consumer should stop or retry instead.
    """
    transient = [OSError, TimeoutError, psycopg2.OperationalError, psycopg2.InterfaceError]
    try:
        from kafka.errors import KafkaError

        transient.append(KafkaError)
    except ImportError:
        pass
    return "transient" if isinstance(exc, tuple(transient)) else "permanent"


def replay_context(record):
    """If this record is a replay, returns (dlq_id, attempts_so_far); else (None, 0)."""
    headers = {}
    for key, value in (getattr(record, "headers", None) or []):
        headers[key] = value
    try:
        dlq_id = int(headers[HEADER_ID].decode())
        attempts = int(headers.get(HEADER_ATTEMPT, b"1").decode())
        return dlq_id, attempts
    except (KeyError, ValueError, AttributeError):
        return None, 0


def build_envelope(row):
    """The JSON published to the DLQ topic. It carries everything needed to debug the failure."""
    return {
        "dlq_id": row["id"],
        "original_message": row["original_message"],
        "original_message_b64": base64.b64encode(row["original_payload"]).decode(),
        "error_type": row["error_type"],
        "error_message": row["error_message"],
        "consumer": row["consumer"],
        "instance": row["instance"],
        "failed_at": iso(row["failed_at"]),
        "attempt_count": row["attempt_count"],
        "status": row["status"],
        "source_topic": row["source_topic"],
        "source_partition": row["source_partition"],
        "source_offset": row["source_offset"],
    }


# ---------------------------------------------------------------- the store

class DeadLetterStore:
    def __init__(self, conn, max_attempts=MAX_ATTEMPTS):
        self.conn = conn
        self.conn.autocommit = True  # every statement is its own transaction
        self.max_attempts = max_attempts
        with self.conn.cursor() as cur:
            cur.execute(DDL)

    @classmethod
    def connect(cls, host, port, user, password, dbname, **kw):
        return cls(psycopg2.connect(host=host, port=port, user=user, password=password,
                                    dbname=dbname, connect_timeout=5), **kw)

    def close(self):
        self.conn.close()

    def _cur(self):
        return self.conn.cursor(cursor_factory=RealDictCursor)

    @staticmethod
    def _clean(row):
        if row is not None:
            row = dict(row)
            row["original_payload"] = bytes(row["original_payload"])
        return row

    # ---- writing failures (called by the consumer)

    def record_failure(self, topic, partition, offset, payload, error_type, error_message,
                       consumer, instance=1, replay_of=None, prior_attempts=0):
        """Records a failure and returns the row.

        A fresh failure inserts a row (inserting the same Kafka offset twice is a
        no-op, so a consumer restart can't create duplicates). A failed REPLAY
        updates the existing row instead of adding a new one."""
        attempts = prior_attempts + 1
        status = "poison" if attempts >= self.max_attempts else "pending"
        with self._cur() as cur:
            if replay_of is not None:
                cur.execute(
                    "UPDATE dlq_records SET attempt_count = %s, error_type = %s, error_message = %s, "
                    "failed_at = now(), consumer = %s, instance = %s, "
                    "status = CASE WHEN status = 'discarded' THEN 'discarded' ELSE %s END "
                    f"WHERE id = %s RETURNING {COLUMNS}",
                    (attempts, error_type, error_message, consumer, instance, status, replay_of))
                row = cur.fetchone()
                if row:
                    return self._clean(row)
            cur.execute(
                "INSERT INTO dlq_records (source_topic, source_partition, source_offset, original_message, "
                "original_payload, error_type, error_message, consumer, instance, attempt_count, status) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (source_topic, source_partition, source_offset) "
                f"DO UPDATE SET source_offset = EXCLUDED.source_offset RETURNING {COLUMNS}",
                (topic, partition, offset, readable(payload), psycopg2.Binary(payload), error_type,
                 error_message, consumer, instance, attempts, status))
            return self._clean(cur.fetchone())

    def mark_resolved(self, dlq_id):
        """A replayed message was processed successfully."""
        with self._cur() as cur:
            cur.execute("UPDATE dlq_records SET status = 'resolved', resolved_at = now() "
                        "WHERE id = %s AND status IN ('replayed', 'pending')", (dlq_id,))

    # ---- reading (called by dlq.py)

    def get(self, dlq_id):
        with self._cur() as cur:
            cur.execute(f"SELECT {COLUMNS} FROM dlq_records WHERE id = %s", (dlq_id,))
            return self._clean(cur.fetchone())

    def list(self, limit=20, status=None, since_seconds=None):
        where, args = [], []
        if status:
            where.append("status = %s")
            args.append(status)
        if since_seconds is not None:
            where.append("failed_at >= now() - (%s * interval '1 second')")
            args.append(since_seconds)
        clause = ("WHERE " + " AND ".join(where)) if where else ""
        with self._cur() as cur:
            cur.execute(f"SELECT {COLUMNS} FROM dlq_records {clause} ORDER BY failed_at DESC, id DESC LIMIT %s",
                        (*args, limit))
            return [self._clean(r) for r in cur.fetchall()]

    def summary(self):
        """Counts per error type and status (the breakdown for the share query)."""
        with self._cur() as cur:
            cur.execute("SELECT error_type, status, count(*) AS n FROM dlq_records GROUP BY 1, 2 ORDER BY 1, 2")
            return cur.fetchall()

    # ---- replay and discard

    def begin_replay(self, dlq_id, force=False):
        """Checks a record may be replayed and marks it 'replayed' BEFORE anything is
        published (so a fast consumer can't resolve it before we mark it).
        Returns the row, with the status it had before under 'previous_status'."""
        row = self.get(dlq_id)
        if row is None:
            raise DlqError(f"No DLQ record with id {dlq_id}")
        status = row["status"]
        if status == "discarded":
            raise DlqError(f"Record {dlq_id} was discarded, so it won't be replayed")
        if status == "resolved":
            raise DlqError(f"Record {dlq_id} is already resolved")
        if status == "replayed":
            raise DlqError(f"Record {dlq_id} was already replayed and is waiting for the consumer")
        if status == "poison" and not force:
            raise DlqError(
                f"Record {dlq_id} is a poison pill (failed {row['attempt_count']} times). Fix the message and "
                "replay it with --payload/--payload-file and --force, or discard it")
        with self._cur() as cur:
            cur.execute("UPDATE dlq_records SET status = 'replayed', replay_count = replay_count + 1, "
                        "last_replayed_at = now() WHERE id = %s AND status = %s", (dlq_id, status))
            if cur.rowcount != 1:
                raise DlqError(f"Record {dlq_id} changed while we were replaying it; try again")
        row["previous_status"] = status
        return row

    def abort_replay(self, dlq_id, previous_status):
        """The publish failed, so put the record back the way it was."""
        with self._cur() as cur:
            cur.execute("UPDATE dlq_records SET status = %s, replay_count = replay_count - 1 "
                        "WHERE id = %s AND status = 'replayed'", (previous_status, dlq_id))

    def discard(self, dlq_id):
        with self._cur() as cur:
            cur.execute("UPDATE dlq_records SET status = 'discarded', resolved_at = now() "
                        "WHERE id = %s AND status NOT IN ('resolved', 'discarded') "
                        f"RETURNING {COLUMNS}", (dlq_id,))
            row = cur.fetchone()
        if row:
            return self._clean(row)
        existing = self.get(dlq_id)
        if existing is None:
            raise DlqError(f"No DLQ record with id {dlq_id}")
        raise DlqError(f"Record {dlq_id} is already {existing['status']}")

    def replayable(self, since_seconds=None):
        """Pending records only: poison, discarded, resolved and in-flight ones are never included."""
        return self.list(limit=1_000_000, status="pending", since_seconds=since_seconds)[::-1]

    def due_for_auto_replay(self, base_delay_seconds):
        """Pending records whose backoff has passed: wait base * 2^(attempts-1) after the last failure."""
        with self._cur() as cur:
            cur.execute(
                f"SELECT {COLUMNS} FROM dlq_records WHERE status = 'pending' AND "
                "failed_at + (%s * interval '1 second') * power(2, attempt_count - 1) <= now() ORDER BY id",
                (base_delay_seconds,))
            return [self._clean(r) for r in cur.fetchall()]


# ---------------------------------------------------------------- what the consumer talks to

class DeadLetterQueue:
    """Sends a failed message to the DLQ topic and to the dlq_records table."""

    def __init__(self, store, producer, topic=DLQ_TOPIC, consumer_name="word_counter", instance=1):
        self.store = store
        self.producer = producer
        self.topic = topic
        self.consumer_name = consumer_name
        self.instance = instance

    def dead_letter(self, record, exc):
        """Saves the failure, publishes the envelope, returns the row. If either step
        fails this raises, so the consumer stops instead of silently losing the message."""
        dlq_id, prior_attempts = replay_context(record)
        row = self.store.record_failure(
            topic=record.topic, partition=record.partition, offset=record.offset,
            payload=record.value or b"", error_type=type(exc).__name__, error_message=str(exc),
            consumer=self.consumer_name, instance=self.instance,
            replay_of=dlq_id, prior_attempts=prior_attempts)
        self.producer.send(self.topic, key=str(row["id"]).encode(), value=build_envelope(row)).get(timeout=10)
        return row

    def message_succeeded(self, record):
        """If this record was a replay, mark its DLQ entry resolved."""
        dlq_id, _ = replay_context(record)
        if dlq_id is not None:
            self.store.mark_resolved(dlq_id)
