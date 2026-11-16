#!/usr/bin/env python3
"""
dlq.py - inspect, replay and discard dead-lettered messages.

    dlq.py list --limit 20                       show recent DLQ records
    dlq.py show 12                               full payload, error and advice
    dlq.py replay 12                             re-publish it to the original topic
    dlq.py replay 12 --payload '{"sentence": "A fixed sentence.", "ts": "2026-01-01T00:00:00Z"}'
    dlq.py replay --all --since 1h               replay every pending record from the last hour
    dlq.py discard 12                            mark as resolved; it will never be replayed
    dlq.py summary                               counts by error type and status
    dlq.py autoreplay --interval 15m             (stretch) keep retrying on a schedule, with backoff

Poison pills (messages that have failed 3 times) are never replayed by --all or
autoreplay. To replay one on purpose, fix it and use: replay ID --payload ... --force
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone

from dlq_store import (HEADER_ATTEMPT, HEADER_ID, MAX_ATTEMPTS, STATUSES, DeadLetterStore, DlqError,
                       parse_duration)

HINTS = {
    "JSONDecodeError": "The text is not valid JSON. Correct it and replay with --payload, or discard it.",
    "UnicodeDecodeError": "The bytes are not valid UTF-8 (often another encoding such as latin-1). "
                          "Re-encode the text as UTF-8 and replay with --payload, or discard it.",
    "ValidationError": "It is valid JSON but a required field ('sentence') is missing or has the wrong type. "
                       "Add it and replay with --payload, or discard it.",
}


def fmt_ts(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S") if dt else "-"


def short(text, n):
    text = text.replace("\n", " ")
    return text if len(text) <= n else text[: n - 3] + "..."


# ---------------------------------------------------------------- commands

def cmd_list(store, args, out=print):
    since = parse_duration(args.since) if args.since else None
    rows = store.list(limit=args.limit, status=args.status, since_seconds=since)
    if not rows:
        out("No DLQ records found.")
        return
    out(f"{'ID':<5} {'FAILED_AT (UTC)':<19} {'STATUS':<9} {'TRIES':<5} {'ERROR_TYPE':<19} ORIGINAL MESSAGE")
    for r in rows:
        tries = f"{r['attempt_count']}/{store.max_attempts}"
        out(f"{r['id']:<5} {fmt_ts(r['failed_at']):<19} {r['status']:<9} {tries:<5} "
            f"{short(r['error_type'], 19):<19} {short(r['original_message'], 50)}")


def cmd_show(store, args, out=print):
    r = store.get(args.id)
    if r is None:
        raise DlqError(f"No DLQ record with id {args.id}")
    out(f"DLQ record {r['id']}")
    out(f"  status:         {r['status']}")
    out(f"  attempts:       {r['attempt_count']} of {store.max_attempts} (replayed {r['replay_count']} time(s))")
    out(f"  consumer:       {r['consumer']} (instance {r['instance']})")
    out(f"  source:         {r['source_topic']} partition {r['source_partition']}, offset {r['source_offset']}")
    out(f"  first failed:   {fmt_ts(r['first_failed_at'])} UTC")
    out(f"  last failed:    {fmt_ts(r['failed_at'])} UTC")
    out(f"  last replayed:  {fmt_ts(r['last_replayed_at'])}" + (" UTC" if r["last_replayed_at"] else ""))
    out(f"  resolved at:    {fmt_ts(r['resolved_at'])}" + (" UTC" if r["resolved_at"] else ""))
    out(f"  error type:     {r['error_type']}")
    out(f"  error message:  {r['error_message']}")
    out(f"  original text:  {r['original_message']}")
    out(f"  original bytes: {r['original_payload'][:300]!r}" + (" ..." if len(r["original_payload"]) > 300 else ""))
    if r["status"] == "poison":
        out(f"  POISON: failed {r['attempt_count']} times, so it is never replayed automatically.")
    hint = HINTS.get(r["error_type"])
    if hint:
        out(f"  what to do:     {hint}")


def replay_one(store, producer, dlq_id, payload=None, force=False):
    """Re-publishes one record to its original topic. The message carries its DLQ id and
    attempt number in headers, so if it fails again the consumer updates THIS record
    (attempt + 1) instead of creating a new one: that is what stops endless loops."""
    row = store.begin_replay(dlq_id, force=force)
    data = payload if payload is not None else row["original_payload"]
    try:
        producer.send(
            row["source_topic"], data,
            headers=[(HEADER_ID, str(dlq_id).encode()), (HEADER_ATTEMPT, str(row["attempt_count"]).encode())],
        ).get(timeout=10)
    except Exception:
        store.abort_replay(dlq_id, row["previous_status"])
        raise
    return row


def cmd_replay(store, producer_factory, args, out=print):
    if args.all:
        if args.payload is not None or args.payload_file or args.id is not None:
            raise DlqError("--all can't be combined with an id or a payload")
        since = parse_duration(args.since) if args.since else None
        rows = store.replayable(since)
        poison = store.list(limit=1_000_000, status="poison", since_seconds=since)
        producer = producer_factory() if rows else None
        replayed = 0
        for row in rows:
            try:
                replay_one(store, producer, row["id"])
                replayed += 1
            except DlqError as e:
                out(f"  skipped {row['id']}: {e}")
        out(f"Replayed {replayed} message(s).")
        if poison:
            out(f"Skipped {len(poison)} poison pill(s): they failed {store.max_attempts} times and are never replayed automatically.")
        return
    if args.id is None:
        raise DlqError("give a DLQ id to replay, or use --all")
    if args.since:
        raise DlqError("--since only works with --all")
    payload = None
    if args.payload is not None:
        payload = args.payload.encode("utf-8")
    elif args.payload_file:
        with open(args.payload_file, "rb") as f:
            payload = f.read()
    row = replay_one(store, producer_factory(), args.id, payload=payload, force=args.force)
    what = "the corrected payload" if payload is not None else "the original message"
    out(f"Replayed record {args.id} ({what}) to topic '{row['source_topic']}'. "
        f"If it fails again, attempt {row['attempt_count'] + 1} of {store.max_attempts} will be recorded"
        + (" and it will become a poison pill." if row["attempt_count"] + 1 >= store.max_attempts else "."))


def cmd_discard(store, args, out=print):
    store.discard(args.id)
    out(f"Discarded record {args.id}. It is marked resolved and will not be replayed.")


def cmd_summary(store, args, out=print):
    counts = {}
    for row in store.summary():
        counts.setdefault(row["error_type"], {s: 0 for s in STATUSES})[row["status"]] = row["n"]
    if not counts:
        out("The dead letter queue is empty.")
        return
    out(f"{'ERROR_TYPE':<22} {'TOTAL':>5} " + " ".join(f"{s.upper():>9}" for s in STATUSES))
    for error_type, by_status in sorted(counts.items()):
        out(f"{error_type:<22} {sum(by_status.values()):>5} " + " ".join(f"{by_status[s]:>9}" for s in STATUSES))


def cmd_autoreplay(store, producer_factory, args, out=print, sleep=time.sleep):
    """Retries pending records on a schedule. Each message waits base * 2^(attempts-1)
    after its last failure, so a message that keeps failing is retried less and less often,
    and after MAX_ATTEMPTS failures it becomes a poison pill and is left alone."""
    interval = parse_duration(args.interval)
    base = parse_duration(args.base_delay)
    producer = None
    out(f"Auto-replay: checking every {args.interval}; a message waits {args.base_delay} x 2^(failures-1) "
        f"after each failure. Ctrl+C to stop.")
    try:
        while True:
            due = store.due_for_auto_replay(base)
            if due and producer is None:
                producer = producer_factory()
            for row in due:
                try:
                    replay_one(store, producer, row["id"])
                    out(f"[{datetime.now().strftime('%H:%M:%S')}] replayed record {row['id']} "
                        f"(failure {row['attempt_count']} of {store.max_attempts})")
                except DlqError as e:
                    out(f"  skipped {row['id']}: {e}")
            if not due:
                out(f"[{datetime.now().strftime('%H:%M:%S')}] nothing due")
            if args.once:
                return
            sleep(interval)
    except KeyboardInterrupt:
        out("\nAuto-replay stopped.")


# ---------------------------------------------------------------- command line

def build_parser():
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--bootstrap", default="localhost:9092")
    common.add_argument("--pg-host", default="localhost")
    common.add_argument("--pg-port", type=int, default=5432)
    common.add_argument("--pg-user", default="postgres")
    common.add_argument("--pg-password", default=os.environ.get("WORDCOUNT_PG_PASSWORD", "password"),
                        help="defaults to $WORDCOUNT_PG_PASSWORD, then the local dev container's password")
    common.add_argument("--pg-db", default="wordcount")
    common.add_argument("--max-attempts", type=int, default=MAX_ATTEMPTS, help="failures before a message is poison")

    parser = argparse.ArgumentParser(prog="dlq.py", description="Inspect, replay and discard dead-lettered messages.")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("list", parents=[common], help="show recent DLQ records")
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--status", choices=STATUSES)
    p.add_argument("--since", metavar="1h", help="only records that failed within this long, e.g. 30m, 1h, 2d")

    p = sub.add_parser("show", parents=[common], help="show one record in full")
    p.add_argument("id", type=int)

    p = sub.add_parser("replay", parents=[common], help="re-publish to the original topic")
    p.add_argument("id", type=int, nargs="?")
    p.add_argument("--all", action="store_true", help="replay every pending record")
    p.add_argument("--since", metavar="1h", help="with --all: only records that failed within this long")
    p.add_argument("--payload", help="replay this corrected message text instead of the original")
    p.add_argument("--payload-file", help="replay the contents of this file instead of the original")
    p.add_argument("--force", action="store_true", help="allow replaying a poison pill (after you have fixed it)")

    p = sub.add_parser("discard", parents=[common], help="mark resolved; it will never be replayed")
    p.add_argument("id", type=int)

    sub.add_parser("summary", parents=[common], help="counts by error type and status")

    p = sub.add_parser("autoreplay", parents=[common], help="keep retrying pending records on a schedule")
    p.add_argument("--interval", default="15m", help="how often to check (default 15m)")
    p.add_argument("--base-delay", default="15m", help="first wait after a failure; doubles each time (default 15m)")
    p.add_argument("--once", action="store_true", help="check once and exit")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    store = None
    try:
        store = DeadLetterStore.connect(args.pg_host, args.pg_port, args.pg_user, args.pg_password,
                                        args.pg_db, max_attempts=args.max_attempts)

        def producer_factory():
            from kafka import KafkaProducer

            return KafkaProducer(bootstrap_servers=args.bootstrap, acks="all")

        if args.command == "list":
            cmd_list(store, args)
        elif args.command == "show":
            cmd_show(store, args)
        elif args.command == "replay":
            cmd_replay(store, producer_factory, args)
        elif args.command == "discard":
            cmd_discard(store, args)
        elif args.command == "summary":
            cmd_summary(store, args)
        elif args.command == "autoreplay":
            cmd_autoreplay(store, producer_factory, args)
        return 0
    except DlqError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        import psycopg2

        if isinstance(e, psycopg2.OperationalError):
            print(f"Couldn't connect to Postgres ({str(e).strip()}).\nTry: sudo docker start pg-cdc", file=sys.stderr)
        else:
            print(f"Error: {type(e).__name__}: {e}", file=sys.stderr)
        return 1
    finally:
        if store is not None:
            store.close()


if __name__ == "__main__":
    sys.exit(main())
