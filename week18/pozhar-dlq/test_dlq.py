"""
Tests for the dead letter queue. They need Postgres:

    export PG_CONN=postgresql://postgres:password@localhost:5432/dlq_test
    python3 -m unittest test_dlq -v

Kafka is replaced by a stand-in that records what would have been published,
so the tests are fast and exact. (They truncate the dlq_records table!)
"""

import base64
import contextlib
import io
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

import psycopg2
from psycopg2.extensions import parse_dsn

import consumer as c
import dlq
import producer
from dlq_store import (HEADER_ATTEMPT, HEADER_ID, MAX_ATTEMPTS, DeadLetterQueue, DeadLetterStore, DlqError,
                       build_envelope, classify, parse_duration, readable, replay_context)

PG_CONN = os.environ.get("PG_CONN")
T = "sentences"


def good(sentence):
    return json.dumps({"sentence": sentence, "ts": "2026-01-01T00:00:00Z"}).encode()


MALFORMED = b'{"sentence": "It was the best of ti'
NO_FIELD = b'{"text": "a sentence under the wrong name", "ts": "2026-01-01T00:00:00Z"}'
NON_UTF8 = '{"sentence": "a caf\u00e9", "ts": "2026-01-01T00:00:00Z"}'.encode("latin-1")


def rec(partition, offset, value, headers=None):
    return SimpleNamespace(topic=T, partition=partition, offset=offset, value=value, headers=headers or [])


def batch(*records):
    d = {}
    for r in records:
        d.setdefault((r.topic, r.partition), []).append(r)
    return d


class FakeProducer:
    """Stands in for KafkaProducer: remembers every message instead of sending it."""

    def __init__(self):
        self.sent, self.fail = [], False

    def send(self, topic, value=None, key=None, headers=None):
        if self.fail:
            raise ConnectionError("kafka is down")
        msg = SimpleNamespace(topic=topic, value=value, key=key, headers=headers or [])
        self.sent.append(msg)
        n = len(self.sent) - 1
        return SimpleNamespace(get=lambda timeout=None: SimpleNamespace(partition=n % 3, offset=n))

    def flush(self):
        pass


class FakeSink:
    def __init__(self):
        self.writes = []

    def write_window(self, start, end, rows, instance):
        self.writes.append((start, end, rows, instance))


class Commits:
    def __init__(self):
        self.calls = []

    def __call__(self, counted):
        self.calls.append(dict(counted))
        return True


def ns(**kw):
    base = dict(id=None, all=False, since=None, payload=None, payload_file=None, force=False,
                limit=20, status=None, interval="15m", base_delay="15m", once=True)
    base.update(kw)
    return SimpleNamespace(**base)


# ======================================================================= no database needed

class ParsingAndClassification(unittest.TestCase):
    def test_a_good_message_is_parsed(self):
        self.assertEqual(c.parse_message(good("It was the best of times")), ["it", "was", "the", "best", "of", "times"])

    def test_each_kind_of_bad_message_fails_in_its_own_way(self):
        cases = [
            (MALFORMED, json.JSONDecodeError),
            (NON_UTF8, UnicodeDecodeError),
            (NO_FIELD, c.ValidationError),
            (b'{"sentence": null}', c.ValidationError),
            (b'{"sentence": 42}', c.ValidationError),
            (b"[1, 2]", c.ValidationError),
            (b"", json.JSONDecodeError),
            (None, c.ValidationError),
        ]
        for raw, exc_type in cases:
            with self.subTest(raw=raw), self.assertRaises(exc_type):
                c.parse_message(raw)

    def test_permanent_vs_transient(self):
        from kafka.errors import KafkaTimeoutError

        for exc in (json.JSONDecodeError("x", "y", 0), UnicodeDecodeError("utf-8", b"\xe9", 0, 1, "bad"),
                    c.ValidationError("x"), KeyError("sentence"), ValueError("x"), TypeError("x")):
            self.assertEqual(classify(exc), "permanent", repr(exc))
        for exc in (psycopg2.OperationalError("down"), ConnectionError("reset"), TimeoutError("slow"),
                    OSError("disk"), KafkaTimeoutError("no broker")):
            self.assertEqual(classify(exc), "transient", repr(exc))

    def test_helpers(self):
        self.assertEqual(parse_duration("15m"), 900)
        self.assertEqual(parse_duration("1h"), 3600)
        with self.assertRaises(DlqError):
            parse_duration("soon")
        self.assertIn("\\xe9", readable(NON_UTF8))
        self.assertEqual(replay_context(rec(0, 0, b"", headers=[(HEADER_ID, b"7"), (HEADER_ATTEMPT, b"2")])), (7, 2))
        self.assertEqual(replay_context(rec(0, 0, b"")), (None, 0))
        self.assertEqual(replay_context(rec(0, 0, b"", headers=[(HEADER_ID, b"oops")])), (None, 0))


class ProducerInjection(unittest.TestCase):
    def test_every_bad_kind_is_really_bad_in_the_right_way(self):
        expected = {"malformed": json.JSONDecodeError, "missing_field": c.ValidationError,
                    "non_utf8": UnicodeDecodeError}
        for kind in producer.BAD_KINDS:
            with self.subTest(kind=kind), self.assertRaises(expected[kind]):
                c.parse_message(producer.make_bad_message(kind, "The sea was calm."))
        with self.assertRaises(ValueError):
            producer.make_bad_message("nonsense", "x")

    def run_dry(self, *args):
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as f:
            f.write("*** START OF THE PROJECT GUTENBERG EBOOK X ***\n"
                    + " ".join(f"Sentence number {i} is here." for i in range(40))
                    + "\n*** END OF THE PROJECT GUTENBERG EBOOK X ***\n")
        self.addCleanup(os.unlink, f.name)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            producer.sys.argv = ["producer.py", "--file", f.name, "--dry-run", "--min-delay", "0",
                                 "--max-delay", "0", *args]
            producer.main()
        return out.getvalue().splitlines()

    def test_inject_bad_sends_n_bad_messages_first_cycling_the_kinds(self):
        lines = self.run_dry("--inject-bad", "6", "--limit", "5")
        bad = [l for l in lines if l.startswith("BAD")]
        self.assertEqual(len(bad), 6)
        self.assertEqual([l.split(":")[0] for l in bad[:3]], ["BAD (malformed)", "BAD (missing_field)", "BAD (non_utf8)"])
        self.assertTrue(all(l.startswith("BAD") for l in lines[1:7]))      # they come before the stream
        self.assertEqual(len([l for l in lines if l.startswith("ok")]), 5)

    def test_bad_rate_mixes_bad_messages_into_the_stream_repeatably(self):
        a = self.run_dry("--limit", "30", "--bad-rate", "0.3", "--seed", "4")
        b = self.run_dry("--limit", "30", "--bad-rate", "0.3", "--seed", "4")
        self.assertEqual([l.split(":")[0] for l in a[1:]], [l.split(":")[0] for l in b[1:]])   # same seed, same pattern
        n_bad = len([l for l in a if l.startswith("BAD")])
        self.assertTrue(3 <= n_bad <= 18, n_bad)
        none = self.run_dry("--limit", "20")
        self.assertEqual(len([l for l in none if l.startswith("BAD")]), 0)

    def test_only_the_requested_kinds_are_used(self):
        lines = self.run_dry("--inject-bad", "4", "--bad-kinds", "non_utf8", "--limit", "1")
        self.assertEqual({l.split(":")[0] for l in lines if l.startswith("BAD")}, {"BAD (non_utf8)"})


# ======================================================================= database tests

@unittest.skipUnless(PG_CONN, "set PG_CONN to run the database tests")
class DlqTests(unittest.TestCase):
    def setUp(self):
        self.store = DeadLetterStore(psycopg2.connect(PG_CONN))
        with self.store.conn.cursor() as cur:
            cur.execute("TRUNCATE dlq_records RESTART IDENTITY")
        self.addCleanup(self.store.close)
        self.dlq_producer = FakeProducer()      # the sentences.dlq topic
        self.replay_producer = FakeProducer()   # the original topic, as used by `dlq.py replay`
        self.lines = []
        self.sink, self.commits = FakeSink(), Commits()
        self.dlq = DeadLetterQueue(self.store, self.dlq_producer, instance=1)
        self.counter = c.WindowedWordCounter(self.sink, 1, self.commits, 30, 10, out=self.lines.append, dlq=self.dlq)
        self.clock = 3001.0

    # ---- helpers
    def feed(self, *records):
        self.counter.process(batch(*records) if records else {}, self.clock)
        self.clock += 31

    def out(self):
        lines = []
        return lines, lines.append

    def command(self, fn, **kw):
        lines, out = self.out()
        fn(self.store, ns(**kw), out=out) if fn in (dlq.cmd_list, dlq.cmd_show, dlq.cmd_discard, dlq.cmd_summary) \
            else fn(self.store, lambda: self.replay_producer, ns(**kw), out=out)
        return "\n".join(lines)

    def replay_cmd(self, **kw):
        return self.command(dlq.cmd_replay, **kw)

    def redeliver_last_replay(self, partition=2, offset=900):
        """The consumer receives what `dlq.py replay` just published (same bytes and headers)."""
        sent = self.replay_producer.sent[-1]
        self.feed(rec(partition, offset, sent.value, headers=sent.headers))

    def row(self, dlq_id=1):
        return self.store.get(dlq_id)

    def dead_letter(self, raw=MALFORMED, offset=0):
        self.feed(rec(1, offset, raw))
        return self.store.list(limit=1)[0]

    # ---- Definition of done: bad messages go to the DLQ and the consumer keeps running
    def test_bad_messages_are_dead_lettered_and_good_ones_keep_flowing(self):
        self.feed(rec(0, 0, good("the cat")), rec(0, 1, MALFORMED), rec(0, 2, good("the dog")),
                  rec(0, 3, NO_FIELD), rec(0, 4, NON_UTF8), rec(0, 5, good("the end")))
        self.feed()                                                  # a quiet poll closes the window
        rows = self.store.list()
        self.assertEqual(sorted(r["error_type"] for r in rows), ["JSONDecodeError", "UnicodeDecodeError", "ValidationError"])
        self.assertEqual(len(self.dlq_producer.sent), 3)             # and each one went to the DLQ topic too
        self.assertEqual(self.sink.writes[0][2][0], ("the", 3))      # the 3 good sentences were all counted
        self.assertEqual(self.counter.skipped, 3)
        self.assertEqual(self.commits.calls[-1], {(T, 0): 6})        # the offset moved past the bad messages

    # ---- Definition of done: the envelope has enough context to debug
    def test_the_envelope_carries_what_you_need_to_debug(self):
        self.dead_letter(NO_FIELD, offset=41)
        env = json.loads(json.dumps(self.dlq_producer.sent[0].value))
        self.assertEqual(self.dlq_producer.sent[0].topic, "sentences.dlq")
        self.assertEqual(env["original_message"], NO_FIELD.decode())
        self.assertEqual(env["error_type"], "ValidationError")
        self.assertIn("sentence", env["error_message"])
        self.assertEqual(env["consumer"], "word_counter")
        self.assertEqual(env["attempt_count"], 1)
        self.assertRegex(env["failed_at"], r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$")
        self.assertEqual((env["source_topic"], env["source_partition"], env["source_offset"]), (T, 1, 41))
        self.assertEqual(base64.b64decode(env["original_message_b64"]), NO_FIELD)
        self.assertEqual(env["dlq_id"], self.row()["id"])

    def test_postgres_copy_matches_and_keeps_the_exact_bytes(self):
        r = self.dead_letter(NON_UTF8)
        self.assertEqual(r["original_payload"], NON_UTF8)           # exact bytes, so replay is faithful
        self.assertIn("\\xe9", r["original_message"])               # readable copy, not a crash
        self.assertEqual((r["status"], r["attempt_count"], r["consumer"]), ("pending", 1, "word_counter"))
        self.assertIn("utf-8", r["error_message"])

    def test_a_restart_that_rereads_a_bad_message_does_not_duplicate_the_record(self):
        record = rec(1, 7, MALFORMED)
        self.feed(record)
        self.feed(record)                                           # same partition and offset again
        self.assertEqual(len(self.store.list()), 1)

    # ---- permanent vs transient
    def test_a_system_failure_stops_the_consumer_and_is_not_blamed_on_the_message(self):
        original = c.parse_message
        c.parse_message = lambda raw: (_ for _ in ()).throw(ConnectionError("network dropped"))
        self.addCleanup(setattr, c, "parse_message", original)
        with self.assertRaises(ConnectionError):
            self.feed(rec(0, 0, good("a perfectly good message")))
        self.assertEqual(self.store.list(), [])                     # NOT dead-lettered
        self.assertEqual(self.dlq_producer.sent, [])
        self.assertEqual(self.counter.counted, {})                  # and not counted as handled

    def test_if_the_dlq_itself_cannot_be_written_the_consumer_stops_and_nothing_is_committed(self):
        self.dlq_producer.fail = True                               # the DLQ topic is unreachable
        with self.assertRaises(ConnectionError):
            self.feed(rec(0, 0, good("fine")), rec(0, 1, MALFORMED))
        self.assertEqual(self.counter.counted.get((T, 0)), 1)       # the good message before it was handled...
        self.assertNotIn(2, self.counter.counted.values())          # ...but the bad one was NOT marked as done
        self.assertEqual(self.commits.calls, [])
        self.dlq_producer.fail = False
        self.feed(rec(0, 1, MALFORMED))                             # after recovery it is dead-lettered once
        self.assertEqual(len(self.store.list()), 1)

    # ---- replay
    def test_replay_republishes_the_exact_bytes_with_its_id_and_attempt(self):
        r = self.dead_letter(NON_UTF8)
        text = self.replay_cmd(id=r["id"])
        sent = self.replay_producer.sent[0]
        self.assertEqual((sent.topic, sent.value), (T, NON_UTF8))
        self.assertEqual(dict(sent.headers), {HEADER_ID: str(r["id"]).encode(), HEADER_ATTEMPT: b"1"})
        after = self.row(r["id"])
        self.assertEqual((after["status"], after["replay_count"]), ("replayed", 1))
        self.assertIn(str(r["id"]), text)
        with self.assertRaises(DlqError):                           # can't replay while it's in flight
            self.replay_cmd(id=r["id"])

    def test_replaying_a_fixed_message_resolves_the_record(self):
        r = self.dead_letter(MALFORMED)
        self.replay_cmd(id=r["id"], payload=good("It was the best of times").decode())
        self.redeliver_last_replay()
        self.feed()
        after = self.row(r["id"])
        self.assertEqual(after["status"], "resolved")
        self.assertIsNotNone(after["resolved_at"])
        self.assertEqual(self.sink.writes[-1][2][0][0], "best")     # the fixed sentence's words were counted
        self.assertEqual(len(self.store.list()), 1)                 # and no new record appeared

    def test_replaying_the_original_works_once_the_code_is_fixed(self):
        original = c.parse_message
        c.parse_message = lambda raw: (_ for _ in ()).throw(ValueError("a bug in version 1"))
        r = self.dead_letter(good("a good sentence the bug rejected"))
        c.parse_message = original                                  # we deployed the fix
        self.replay_cmd(id=r["id"])
        self.redeliver_last_replay()
        self.assertEqual(self.row(r["id"])["status"], "resolved")

    # ---- a replay that fails again must not loop
    def test_a_replay_that_fails_again_updates_the_same_record_and_becomes_poison_after_three_failures(self):
        r = self.dead_letter(MALFORMED)
        self.assertEqual(r["attempt_count"], 1)
        self.replay_cmd(id=r["id"])
        self.redeliver_last_replay(offset=901)                      # fails again
        second = self.row(r["id"])
        self.assertEqual((second["attempt_count"], second["status"]), (2, "pending"))
        self.assertEqual(len(self.store.list()), 1)                 # still ONE record: no loop, no duplicate
        self.replay_cmd(id=r["id"])
        self.redeliver_last_replay(offset=902)                      # fails a third time
        third = self.row(r["id"])
        self.assertEqual((third["attempt_count"], third["status"]), (MAX_ATTEMPTS, "poison"))
        self.assertEqual(len(self.store.list()), 1)
        self.assertEqual([e.value["attempt_count"] for e in self.dlq_producer.sent], [1, 2, 3])
        self.assertTrue(any("POISON" in l for l in self.lines))

    def test_poison_pills_are_isolated(self):
        poison = self.dead_letter(MALFORMED, offset=0)
        for i in range(2):
            self.replay_cmd(id=poison["id"])
            self.redeliver_last_replay(offset=800 + i)
        self.assertEqual(self.row(poison["id"])["status"], "poison")
        pending = self.dead_letter(NO_FIELD, offset=5)
        self.replay_producer.sent.clear()
        text = self.replay_cmd(all=True)                            # bulk replay
        self.assertEqual(len(self.replay_producer.sent), 1)         # only the pending one
        self.assertEqual(dict(self.replay_producer.sent[0].headers)[HEADER_ID], str(pending["id"]).encode())
        self.assertIn("poison", text.lower())
        with self.assertRaises(DlqError) as cm:                     # and a manual replay needs a deliberate override
            self.replay_cmd(id=poison["id"])
        self.assertIn("poison", str(cm.exception).lower())
        self.replay_cmd(id=poison["id"], payload=good("repaired now").decode(), force=True)
        self.redeliver_last_replay(offset=999)
        self.assertEqual(self.row(poison["id"])["status"], "resolved")

    # ---- discard
    def test_discarded_records_are_never_replayed(self):
        keep, drop = self.dead_letter(MALFORMED, 0), self.dead_letter(NO_FIELD, 1)
        self.command(dlq.cmd_discard, id=drop["id"])
        self.assertEqual(self.row(drop["id"])["status"], "discarded")
        self.assertIsNotNone(self.row(drop["id"])["resolved_at"])
        with self.assertRaises(DlqError):
            self.replay_cmd(id=drop["id"])
        self.replay_cmd(all=True)
        self.assertEqual(len(self.replay_producer.sent), 1)
        with self.assertRaises(DlqError):
            self.command(dlq.cmd_discard, id=drop["id"])            # already done
        with self.assertRaises(DlqError):
            self.command(dlq.cmd_discard, id=9999)                  # no such record

    def test_bulk_replay_can_be_limited_to_recent_failures(self):
        old, new = self.dead_letter(MALFORMED, 0), self.dead_letter(NO_FIELD, 1)
        with self.store.conn.cursor() as cur:
            cur.execute("UPDATE dlq_records SET failed_at = now() - interval '3 hours' WHERE id = %s", (old["id"],))
        self.replay_cmd(all=True, since="1h")
        self.assertEqual([dict(m.headers)[HEADER_ID] for m in self.replay_producer.sent], [str(new["id"]).encode()])
        self.assertEqual(self.row(old["id"])["status"], "pending")

    def test_a_failed_publish_leaves_the_record_replayable(self):
        r = self.dead_letter(MALFORMED)
        self.replay_producer.fail = True
        with self.assertRaises(ConnectionError):
            self.replay_cmd(id=r["id"])
        after = self.row(r["id"])
        self.assertEqual((after["status"], after["replay_count"]), ("pending", 0))
        self.replay_producer.fail = False
        self.replay_cmd(id=r["id"])                                 # so it can simply be retried
        self.assertEqual(self.row(r["id"])["status"], "replayed")

    def test_replay_argument_mistakes_are_clear(self):
        for kw in (dict(), dict(id=1, all=True), dict(all=True, payload="x"), dict(id=12345)):
            with self.subTest(kw=kw), self.assertRaises(DlqError):
                self.replay_cmd(**kw)

    # ---- list / show / summary
    def test_list_show_and_summary(self):
        a, b = self.dead_letter(MALFORMED, 0), self.dead_letter(NO_FIELD, 1)
        self.dead_letter(NON_UTF8, 2)
        self.command(dlq.cmd_discard, id=b["id"])
        listing = self.command(dlq.cmd_list, limit=20)
        for needle in ("ID", "STATUS", "JSONDecodeError", "ValidationError", "UnicodeDecodeError", "discarded", "pending"):
            self.assertIn(needle, listing)
        self.assertEqual(self.command(dlq.cmd_list, status="discarded").count("ValidationError"), 1)
        self.assertEqual(self.command(dlq.cmd_list, limit=2).count("\n"), 2)       # header + 2 rows
        self.assertIn("No DLQ records", self.command(dlq.cmd_list, status="poison"))
        shown = self.command(dlq.cmd_show, id=a["id"])
        for needle in ("JSONDecodeError", "Unterminated string", "b'{\"sentence\"", "pending", "what to do"):
            self.assertIn(needle, shown)
        self.assertIn("\\xe9", self.command(dlq.cmd_show, id=3))                    # undecodable bytes shown safely
        with self.assertRaises(DlqError):
            self.command(dlq.cmd_show, id=999)
        summary = self.command(dlq.cmd_summary)
        self.assertRegex(summary, r"JSONDecodeError\s+1")
        self.assertRegex(summary, r"ValidationError\s+1")

    # ---- stretch goal: auto-replay with exponential backoff per message
    def test_auto_replay_backs_off_per_message(self):
        r = self.dead_letter(MALFORMED)

        def age(seconds):
            with self.store.conn.cursor() as cur:
                cur.execute("UPDATE dlq_records SET failed_at = now() - (%s * interval '1 second') WHERE id = 1", (seconds,))

        due = lambda: [x["id"] for x in self.store.due_for_auto_replay(60)]
        self.assertEqual(due(), [])                                 # just failed: not yet
        age(61)
        self.assertEqual(due(), [r["id"]])                          # attempt 1 waits 60s
        self.replay_cmd(id=1)
        self.redeliver_last_replay(offset=910)                      # fails again: attempt 2
        age(61)
        self.assertEqual(due(), [])                                 # attempt 2 now waits 120s
        age(121)
        self.assertEqual(due(), [r["id"]])
        self.replay_cmd(id=1)
        self.redeliver_last_replay(offset=911)                      # attempt 3: poison
        age(100000)
        self.assertEqual(due(), [])                                 # poison is never retried automatically

    def test_autoreplay_command_replays_due_records_and_loops_until_stopped(self):
        r = self.dead_letter(MALFORMED)
        with self.store.conn.cursor() as cur:
            cur.execute("UPDATE dlq_records SET failed_at = now() - interval '2 minutes'")
        text = self.command(dlq.cmd_autoreplay, base_delay="1m", once=True)
        self.assertEqual(len(self.replay_producer.sent), 1)
        self.assertEqual(self.row(r["id"])["status"], "replayed")
        self.assertIn("replayed", text)

        sleeps = []

        def sleep(seconds):
            sleeps.append(seconds)
            if len(sleeps) == 2:
                raise KeyboardInterrupt

        lines, out = self.out()
        dlq.cmd_autoreplay(self.store, lambda: self.replay_producer, ns(interval="15m", base_delay="1m", once=False),
                           out=out, sleep=sleep)
        self.assertEqual(sleeps, [900, 900])
        self.assertIn("stopped", "\n".join(lines).lower())

    # ---- the command line itself
    def test_command_line_end_to_end(self):
        r = self.dead_letter(MALFORMED)
        dsn = parse_dsn(PG_CONN)
        def conn_args_for(port):
            return ["--pg-host", dsn.get("host", "localhost"), "--pg-port", str(port),
                    "--pg-user", dsn["user"], "--pg-password", dsn["password"], "--pg-db", dsn["dbname"]]

        conn_args = conn_args_for(dsn.get("port", "5432"))
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            self.assertEqual(dlq.main(["list", *conn_args]), 0)
            self.assertEqual(dlq.main(["show", str(r["id"]), *conn_args]), 0)
            self.assertEqual(dlq.main(["summary", *conn_args]), 0)
            self.assertEqual(dlq.main(["discard", str(r["id"]), *conn_args]), 0)
            self.assertEqual(dlq.main(["show", "9999", *conn_args]), 1)
            self.assertEqual(dlq.main(["replay", *conn_args]), 1)       # neither an id nor --all
            self.assertEqual(dlq.main(["list", *conn_args_for(1)]), 1)   # an unreachable database
        self.assertIn("JSONDecodeError", out.getvalue())
        self.assertIn("No DLQ record with id 9999", err.getvalue())
        self.assertIn("Couldn't connect to Postgres", err.getvalue())
        self.assertEqual(self.row(r["id"])["status"], "discarded")


if __name__ == "__main__":
    unittest.main()
