"""
consumer.py - reads sentences from Kafka, counts words in 30-second tumbling
windows, and saves each window's top 10 words to Postgres.

Windows follow the wall clock (a window is :00-:30 or :30-:00 of each minute),
so every consumer in the group agrees on where windows begin and end. A
sentence belongs to the window in which this consumer processed it.

How it avoids double-counting and missing data:
  * Offsets are committed to Kafka only AFTER a window's rows are safely in
    Postgres, and only for the sentences that were in that window. If the
    program dies mid-window, Kafka hands those sentences back on restart.
  * If Kafka takes partitions away from this consumer (another instance
    joined), the unfinished window is thrown away, because the new owner will
    re-read those sentences from the last commit.
  * Ctrl+C also discards the unfinished window, for the same reason.

Usage:
    python3 consumer.py                      # instance 1
    python3 consumer.py --instance 2         # a second consumer, same group
    python3 consumer.py --no-db              # print windows, skip Postgres
"""

import argparse
import json
import os
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone

# letters, with apostrophes allowed inside a word (don't, it's)
WORD_RE = re.compile(r"[a-z]+(?:'[a-z]+)*")


def tokenize(sentence):
    """Lowercase a sentence and return its words, with punctuation stripped."""
    text = sentence.lower().replace("\u2019", "'").replace("\u2018", "'")
    return WORD_RE.findall(text)


def top_words(counts, n):
    """Top n (word, count) pairs; ties are broken alphabetically so results are repeatable."""
    return sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:n]


def fmt_time(epoch):
    return datetime.fromtimestamp(epoch).strftime("%H:%M:%S")


class TumblingWindow:
    """Fixed, non-overlapping time buckets aligned to the clock."""

    def __init__(self, size_seconds=30):
        self.size = size_seconds
        self.start = None  # epoch seconds where the open window began
        self.counts = Counter()
        self.sentences = 0

    def start_for(self, ts):
        return int(ts // self.size) * self.size

    def advance(self, ts):
        """Moves the clock to ts. If ts is past the end of the open window,
        closes it and returns (start, end, counts, sentences); otherwise None."""
        new_start = self.start_for(ts)
        if self.start is None:
            self.start = new_start
            return None
        if new_start == self.start:
            return None
        closed = (self.start, self.start + self.size, self.counts, self.sentences)
        self.start = new_start
        self.counts = Counter()
        self.sentences = 0
        return closed

    def add(self, words):
        self.counts.update(words)
        self.sentences += 1

    def discard(self):
        """Throws the open window's data away; returns how many sentences it held."""
        dropped = self.sentences
        self.counts = Counter()
        self.sentences = 0
        return dropped


class PostgresSink:
    """Writes closed windows to the word_counts_30s table."""

    def __init__(self, conn):
        self.conn = conn

    @classmethod
    def connect(cls, host, port, user, password, dbname):
        import psycopg2

        conn = psycopg2.connect(
            host=host, port=port, user=user, password=password, dbname=dbname, connect_timeout=5
        )
        sink = cls(conn)
        sink.ensure_schema()
        return sink

    def ensure_schema(self):
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(
                    "CREATE TABLE IF NOT EXISTS word_counts_30s ("
                    "window_start TIMESTAMPTZ NOT NULL, window_end TIMESTAMPTZ NOT NULL, "
                    "word TEXT NOT NULL, count INTEGER NOT NULL)"
                )
                # which consumer instance wrote the row (each instance only sees its own partitions)
                cur.execute(
                    "ALTER TABLE word_counts_30s ADD COLUMN IF NOT EXISTS instance INTEGER NOT NULL DEFAULT 1"
                )
                cur.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS word_counts_30s_uniq "
                    "ON word_counts_30s (window_start, instance, word)"
                )

    def write_window(self, start, end, rows, instance):
        start_dt = datetime.fromtimestamp(start, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(end, tz=timezone.utc)
        # one transaction: every row of the window is saved, or none of them
        with self.conn:
            with self.conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO word_counts_30s (window_start, window_end, word, count, instance) "
                    "VALUES (%s, %s, %s, %s, %s) "
                    "ON CONFLICT (window_start, instance, word) DO NOTHING",
                    [(start_dt, end_dt, word, count, instance) for word, count in rows],
                )

    def close(self):
        self.conn.close()


class WindowedWordCounter:
    """Counts words per window and saves each window when it closes."""

    def __init__(self, sink, instance, commit_fn, window_seconds=30, top_n=10, out=print):
        self.sink = sink
        self.instance = instance
        self.commit_fn = commit_fn  # commit_fn({(topic, partition): next_offset}) -> True if it worked
        self.top_n = top_n
        self.out = out
        self.window = TumblingWindow(window_seconds)
        self.counted = {}  # (topic, partition) -> next offset, for sentences handled so far
        self.committed = {}  # what we last committed to Kafka
        self.skipped = 0
        self.last_ts = None

    def process(self, batches, ts):
        """Handles one poll() result that arrived at time ts."""
        self.last_ts = ts
        closed = self.window.advance(ts)
        if closed:
            # Close the old window BEFORE counting this batch, so the offsets we
            # commit cover only sentences that were really in that window.
            self._close(closed)

        for records in batches.values():
            for record in records:
                self.counted[(record.topic, record.partition)] = record.offset + 1
                try:
                    message = json.loads(record.value.decode("utf-8"))
                    words = tokenize(message["sentence"])
                except (ValueError, KeyError, TypeError, AttributeError):
                    self.skipped += 1  # malformed message: skip it
                    continue
                self.window.add(words)

    def _close(self, closed):
        start, end, counts, sentences = closed
        if sentences:
            rows = top_words(counts, self.top_n)
            if self.sink is not None:
                self.sink.write_window(start, end, rows, self.instance)  # raises if Postgres fails
            saved = "" if self.sink is not None else " (not saved: --no-db)"
            self.out(
                f"[{fmt_time(end)}] window {fmt_time(start)}-{fmt_time(end)} closed: "
                f"{sentences} sentences, {sum(counts.values())} words. Top {len(rows)}{saved}:"
            )
            self.out("    " + ", ".join(f"{word}={count}" for word, count in rows))

        # Only now is it safe to tell Kafka these sentences are finished.
        if self.counted != self.committed:
            if self.commit_fn(dict(self.counted)):
                self.committed = dict(self.counted)

    def report(self, top_n=5):
        """Live view of the window that is still open."""
        w = self.window
        if w.start is None or self.last_ts is None:
            return
        span = f"{fmt_time(w.start)}-{fmt_time(w.start + w.size)}"
        if w.sentences == 0:
            self.out(f"[{fmt_time(self.last_ts)}] window {span}: waiting for messages...")
            return
        self.out(
            f"[{fmt_time(self.last_ts)}] window {span} so far: {w.sentences} sentences, "
            f"{sum(w.counts.values())} words. Top {top_n}:"
        )
        for rank, (word, count) in enumerate(top_words(w.counts, top_n), start=1):
            self.out(f"    {rank}. {word:<12} {count}")

    def on_assigned(self, partitions):
        self.out(f"Instance {self.instance} now reads partitions: {sorted(partitions)}")

    def on_revoked(self, partitions):
        """Kafka took partitions away (a rebalance). The new owner will re-read
        everything we haven't committed, so our unfinished window must go."""
        dropped = self.window.discard()
        self.counted = {}
        self.committed = {}
        if partitions or dropped:
            self.out(
                f"Rebalance: partitions {sorted(partitions)} taken away. Dropped the unfinished "
                f"window ({dropped} sentences); they will be re-read by whoever owns them next."
            )

    def shutdown(self):
        dropped = self.window.discard()
        self.out(f"\nStopping. Discarded the unfinished window ({dropped} sentences); they will be re-read next time.")
        if self.skipped:
            self.out(f"(skipped {self.skipped} malformed messages)")


def run(counter, consumer, report_interval=10.0, clock_time=time.time, mono=time.monotonic):
    """Polls Kafka until interrupted."""
    last_report = mono()
    try:
        while True:
            # poll() waits at most 1s, so windows still close on time when no messages arrive
            batches = consumer.poll(timeout_ms=1000)
            counter.process(batches, clock_time())
            if mono() - last_report >= report_interval:
                counter.report()
                last_report = mono()
    except KeyboardInterrupt:
        counter.shutdown()


def commit_offsets(consumer, counted, out=print):
    """Commits {(topic, partition): next_offset} to Kafka. Returns True on success."""
    from kafka import TopicPartition
    from kafka.errors import KafkaError
    from kafka.structs import OffsetAndMetadata

    def make(offset):
        try:
            return OffsetAndMetadata(offset, "", -1)  # newer kafka-python
        except TypeError:
            return OffsetAndMetadata(offset, "")  # older kafka-python

    try:
        consumer.commit({TopicPartition(t, p): make(o) for (t, p), o in counted.items()})
        return True
    except KafkaError as e:
        out(f"Warning: couldn't commit offsets ({type(e).__name__}). If a rebalance is under way, "
            "this window's sentences may be counted again later.")
        return False


def make_rebalance_listener(counter):
    from kafka import ConsumerRebalanceListener

    class Listener(ConsumerRebalanceListener):
        def on_partitions_revoked(self, revoked):
            counter.on_revoked([tp.partition for tp in revoked])

        def on_partitions_lost(self, lost):
            counter.on_revoked([tp.partition for tp in lost])

        def on_partitions_assigned(self, assigned):
            counter.on_assigned([tp.partition for tp in assigned])

    return Listener()


def main():
    parser = argparse.ArgumentParser(description="30-second tumbling-window word counts from Kafka.")
    parser.add_argument("--topic", default="sentences")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--group", default="wordcount-windows", help="consumer group id")
    parser.add_argument("--instance", type=int, default=1, help="instance number (for running several consumers)")
    parser.add_argument("--window-seconds", type=int, default=30)
    parser.add_argument("--top", type=int, default=10, help="words saved per window")
    parser.add_argument("--report-interval", type=float, default=10.0, help="seconds between live reports")
    parser.add_argument("--no-db", action="store_true", help="don't write to Postgres")
    parser.add_argument("--pg-host", default="localhost")
    parser.add_argument("--pg-port", type=int, default=5432)
    parser.add_argument("--pg-user", default="postgres")
    parser.add_argument("--pg-password", default=os.environ.get("WORDCOUNT_PG_PASSWORD", "password"),
                        help="defaults to $WORDCOUNT_PG_PASSWORD, then the local dev container's password")
    parser.add_argument("--pg-db", default="wordcount")
    args = parser.parse_args()

    # check Postgres first, since it fails fast (Kafka can take 30s to give up)
    sink = None
    if not args.no_db:
        try:
            sink = PostgresSink.connect(args.pg_host, args.pg_port, args.pg_user, args.pg_password, args.pg_db)
        except Exception as e:
            print(
                f"Couldn't connect to Postgres at {args.pg_host}:{args.pg_port} ({e}).\n"
                "Try: sudo docker start pg-cdc    (or run with --no-db)",
                file=sys.stderr,
            )
            sys.exit(1)

    from kafka import KafkaConsumer
    from kafka.errors import KafkaError

    print(f"Connecting to Kafka at {args.bootstrap} (gives up after ~30s)...")
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=args.bootstrap,
            group_id=args.group,
            client_id=f"wordcount-{args.instance}",
            enable_auto_commit=False,  # we commit ourselves, only after Postgres has the window
            auto_offset_reset="earliest",  # a brand-new group starts from the beginning
        )
    except KafkaError:
        print(
            f"Couldn't reach Kafka at {args.bootstrap}. Is Redpanda running?\n"
            "Try: sudo docker start redpanda",
            file=sys.stderr,
        )
        sys.exit(1)

    counter = WindowedWordCounter(
        sink=sink,
        instance=args.instance,
        commit_fn=lambda counted: commit_offsets(consumer, counted),
        window_seconds=args.window_seconds,
        top_n=args.top,
    )
    consumer.subscribe([args.topic], listener=make_rebalance_listener(counter))
    print(f"Instance {args.instance} in group '{args.group}', reading '{args.topic}'. Ctrl+C to stop.")

    exit_code = 0
    try:
        run(counter, consumer, report_interval=args.report_interval)
    except Exception as e:
        # most likely a failed Postgres write; offsets were NOT committed, so nothing is lost
        print(f"\nStopped on error: {type(e).__name__}: {e}\n"
              "Offsets for the unsaved window were not committed, so restarting is safe.", file=sys.stderr)
        exit_code = 1
    finally:
        consumer.close(autocommit=False)
        if sink is not None:
            sink.close()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
