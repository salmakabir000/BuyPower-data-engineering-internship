# Streaming Word Counter with a Dead Letter Queue

A producer streams a book into Kafka (Redpanda) one sentence at a time. A consumer
counts words in 30-second tumbling windows and saves each window's top 10 to Postgres.
This version adds a **dead letter queue (DLQ)**: a message that can't be processed is set
aside for a person to inspect, fix and replay, and the consumer carries on.

## Setup

Start Redpanda, then create the topics (the DLQ topic must exist before the consumer starts):

    sudo docker run -d --name=redpanda -p 9092:9092 -p 9644:9644 \
      docker.redpanda.com/redpandadata/redpanda:latest \
      redpanda start --overprovisioned --smp 1 --memory 1G --reserve-memory 0M \
      --node-id 0 --check=false --kafka-addr=PLAINTEXT://0.0.0.0:9092 \
      --advertise-kafka-addr=PLAINTEXT://localhost:9092
    sudo docker exec -it redpanda rpk topic create sentences -p 3
    sudo docker exec -it redpanda rpk topic create sentences.dlq -p 1

Postgres (the tables are created automatically):

    sudo docker exec -it pg-cdc psql -U postgres -c "CREATE DATABASE wordcount;"

Python and the book:

    pip install -r requirements.txt
    curl -o frankenstein.txt https://www.gutenberg.org/cache/epub/84/pg84.txt

## Run

    python3 consumer.py                      # terminal 1
    python3 producer.py --inject-bad 6       # terminal 2: 6 bad messages first, then the book
    python3 producer.py --bad-rate 0.05      # or: about 1 message in 20 is bad
    python3 dlq.py list                      # terminal 3: see what was dead-lettered

The producer's bad messages come in three kinds: `malformed` (JSON cut off halfway),
`missing_field` (valid JSON without `sentence`) and `non_utf8` (latin-1 bytes). Use
`--bad-kinds` to choose which, and `--seed` to repeat a run exactly.

## How the DLQ works

1. The consumer decodes each message, parses the JSON and checks it has a text `sentence`.
2. If that fails, the message is written to the `dlq_records` table and published to
   the `sentences.dlq` topic, and the consumer moves on. Good messages are unaffected.
3. The message's offset is committed only after the DLQ write succeeded, so a bad message
   can never be lost. If the DLQ itself can't be written, the consumer stops instead.

The envelope on `sentences.dlq` (and the matching `dlq_records` row):

    {
      "dlq_id": 12,
      "original_message": "{\"sentence\": \"It was the best of ti",
      "original_message_b64": "eyJzZW50ZW5jZSI6...",
      "error_type": "JSONDecodeError",
      "error_message": "Unterminated string starting at: line 1 column 14 (char 13)",
      "consumer": "word_counter",
      "instance": 1,
      "failed_at": "2026-06-04T10:00:00Z",
      "attempt_count": 1,
      "status": "pending",
      "source_topic": "sentences", "source_partition": 2, "source_offset": 41
    }

`original_message` is readable text (bytes that aren't valid UTF-8 appear as `\xe9`).
`original_message_b64` and the table's `original_payload` hold the exact bytes, which is what
replay re-sends. The source topic, partition and offset say exactly where it came from.

### Statuses

| status | meaning |
|---|---|
| `pending` | failed; waiting for someone to look at it |
| `replayed` | re-published; waiting to see what happens |
| `resolved` | a replay succeeded |
| `discarded` | a person decided it isn't worth fixing; never replayed |
| `poison` | failed 3 times in total; never replayed automatically |

## The inspector: dlq.py

    python3 dlq.py list --limit 20                  # recent records (also --status, --since 1h)
    python3 dlq.py show 12                          # full payload, error and what to do about it
    python3 dlq.py replay 12                        # re-publish the original to the original topic
    python3 dlq.py replay 12 --payload '{"sentence": "It was the best of times.", "ts": "2026-06-04T10:00:00Z"}'
    python3 dlq.py replay 12 --payload-file fixed.json
    python3 dlq.py replay --all --since 1h          # bulk replay pending records from the last hour
    python3 dlq.py discard 12                       # mark resolved; it won't be replayed
    python3 dlq.py summary                          # counts per error type and status

A replayed message is sent with its DLQ id and attempt number in Kafka headers. When the
consumer receives it:

* if it now **succeeds**, the record becomes `resolved`;
* if it **fails again**, the consumer updates the *same* record (attempt_count + 1) instead
  of creating a new one. So a replay can never loop back into the DLQ as a fresh entry.

After **3 failures in total** (the original plus replays) the record becomes `poison`. Poison
pills are skipped by `replay --all` and by auto-replay. To replay one deliberately you must
fix the message and add `--force`, so a human is always the one deciding.

Replaying the *same bytes* only helps if something else has changed (for example you fixed a
bug in the consumer). To fix a bad message itself, replay with `--payload` or `--payload-file`.

## Transient errors vs permanent errors

This decides whether a failure belongs in the DLQ at all.

**Permanent (a poison-pill message):** something about *this message* is wrong, such as invalid
JSON, bytes that aren't UTF-8, or a missing required field. Trying the same message again
without changing it fails the same way every time. It goes to the DLQ, and it needs a fix to the
message (or to the code) before a replay can work. These are the errors the consumer
dead-letters.

**Transient (a downstream or system error):** nothing is wrong with the message. The system
around it had a problem, such as Postgres being down, the network dropping, or Kafka being
unreachable. The same message will work later. It must **not** go to the DLQ. If it did, an
outage of a few minutes would fill the DLQ with perfectly good messages and someone would have
to replay them. Instead the consumer stops, commits nothing, and picks the messages up again
when restarted, and retrying with backoff is the right response.

**How to decide.** Ask: *if I tried this exact message again in a minute, with nothing changed,
could it succeed?*

* **Yes, it might** (the failure depends on the system) means transient: retry, don't dead-letter.
* **No, never** (the failure depends on the message's content) means permanent: dead-letter it.

The code makes this call in `dlq_store.classify()`. The `try/except` in the consumer wraps only
the work on the single message, and anything that classifies as transient is re-raised
instead of dead-lettered. Failures outside it (writing a window to Postgres, committing offsets)
are never blamed on a message, because they aren't about any one message.

Two cases need judgment. A *bug in your own code* is permanent from the message's point of view
(it fails every time) but fixed by deploying the fix and replaying the original. And an error
that looks transient but never goes away (the same record fails the same way on every retry) is
really permanent. That's what the 3-attempt poison rule catches.

## Auto-replay (stretch goal)

    python3 dlq.py autoreplay --interval 15m --base-delay 15m

Every `--interval` it replays pending records that are due. A record that has failed *n* times
waits `base-delay x 2^(n-1)` after its last failure (15 min, then 30 min), so repeat offenders
are retried less and less often, and after 3 failures they become poison and stop. Use
`--once` for a single pass. Auto-replay only helps for failures that can fix themselves.
Replaying a permanently bad message just uses up its attempts until it is isolated as poison.

## Breakdown by error type (SQL)

    SELECT error_type,
           count(*)                                    AS records,
           count(*) FILTER (WHERE status = 'pending')   AS pending,
           count(*) FILTER (WHERE status = 'resolved')  AS resolved,
           count(*) FILTER (WHERE status = 'discarded') AS discarded,
           count(*) FILTER (WHERE status = 'poison')    AS poison
    FROM dlq_records
    GROUP BY error_type
    ORDER BY records DESC;

## Tests

    export PG_CONN=postgresql://postgres:password@localhost:5432/dlq_test
    python3 test_windows.py            # window counting, offsets, crash and rebalance behaviour
    python3 -m unittest test_dlq -v    # the DLQ (note: it truncates dlq_records)

Kafka is replaced by a stand-in in the tests; Postgres is real.

## Known limitations

* **At-least-once.** If the consumer crashes after writing to the DLQ but before committing,
  the message is read again: the table doesn't duplicate it (one row per Kafka offset), but a
  second envelope can appear on the `sentences.dlq` topic. The table is the source of truth.
* A record stays `replayed` until the consumer processes it. If the consumer is stopped, it
  waits there.
* A replayed message is published without a key, so it may land on a different partition.
* The DLQ needs Postgres; with `--no-db` the DLQ is switched off.
* Windows use processing time, and with two consumers each instance's top 10 covers only its
  own partitions (see the Week 15 notes).
