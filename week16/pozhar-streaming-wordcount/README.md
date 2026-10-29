# Streaming Word Counter

Streams a book through Redpanda (Kafka-compatible) and counts words live. A
producer sends Frankenstein one sentence at a time. Consumers count the words
in 30-second tumbling windows and save each window's top 10 words to Postgres.

## Setup

Start Redpanda and create the topic with 3 partitions:

    sudo docker run -d --name=redpanda -p 9092:9092 -p 9644:9644 \
      docker.redpanda.com/redpandadata/redpanda:latest \
      redpanda start --overprovisioned --smp 1 --memory 1G --reserve-memory 0M \
      --node-id 0 --check=false --kafka-addr=PLAINTEXT://0.0.0.0:9092 \
      --advertise-kafka-addr=PLAINTEXT://localhost:9092
    sudo docker exec -it redpanda rpk topic create sentences -p 3

Create a Postgres database (mine runs in Docker as `pg-cdc`):

    sudo docker exec -it pg-cdc psql -U postgres -c "CREATE DATABASE wordcount;"

The consumer creates the `word_counts_30s` table itself on startup. Then:

    pip install kafka-python psycopg2-binary
    curl -o frankenstein.txt https://www.gutenberg.org/cache/epub/84/pg84.txt

## Run

    python3 consumer.py                # terminal 1
    python3 producer.py                # terminal 2
    python3 consumer.py --instance 2   # terminal 3 (optional): joins the same group

Run `python3 consumer.py --help` for the options. The Postgres password comes
from `--pg-password`, then the `WORDCOUNT_PG_PASSWORD` environment variable,
then the default of my local dev container.

## The table

`word_counts_30s` has `window_start`, `window_end`, `word`, `count` and
`instance`. The extra `instance` column records which consumer wrote the row,
so two consumers can write the same window. It is unique on
(window_start, instance, word).

## How the windows work

- Windows follow the wall clock: every window runs from :00 to :30 or :30 to
  :00 of a minute, so all consumers agree where windows start and end.
- A sentence belongs to the window in which the consumer processed it.
- When a window closes, its top 10 words are written to Postgres in one
  transaction (all 10 rows or none). Then the counts reset.
- A window with no sentences writes nothing.
- The live report every 10 seconds shows the window that is still open.

## No double-counting, no missing windows

- Offsets are committed to Kafka only after the window's rows are in Postgres,
  and only for the sentences that were in that window. A crash mid-window means
  Kafka hands those sentences back on restart.
- If Kafka takes partitions away (another consumer joined), the unfinished
  window is discarded, because the new owner re-reads those sentences from the
  last commit. Ctrl+C does the same.
- A unique index plus ON CONFLICT DO NOTHING stops the same window being
  written twice.
- `python3 test_windows.py` runs 10 scenarios against a simulated broker,
  including a crash and restart where the counts across all windows exactly
  match the sentences in the topic.

## Known limitations

- Delivery is at-least-once. A crash or rebalance in the few milliseconds
  between the Postgres write and the offset commit could make one window's
  sentences count again in a later window. Exactly-once would mean storing the
  offsets in Postgres in the same transaction as the counts.
- Windows use processing time (when the consumer sees a sentence), not the
  timestamp inside the message. Replayed sentences land in the window where
  they are processed.
- With two consumers, each one only sees its own partitions, so each
  instance's top 10 covers only part of the data. Combining instances gives an
  approximation of the true top 10.
- Summing `count` for a window adds up only the saved top-10 words, not every
  word in the window.
- The sentence splitter cuts after abbreviations such as "St." This does not
  affect word counts.

## Tumbling windows

A tumbling window is a fixed block of time, 30 seconds in my project, that never overlaps with the next one. My consumer counts the words that arrive during that block. When the 30 seconds are over, it saves the top 10 words to Postgres and starts again from zero, so no sentence is ever counted in two windows. A sliding window is different, because the same sentence can fall into several overlapping windows at once. My windows follow the clock, :00 to :30 and :30 to :00, so every window starts exactly where the last one ended.


## Why partitioning matters

A partition is one lane of a topic. My topic has 3 lanes, and inside a consumer group only one consumer can read a lane at a time. With a single lane, one consumer has to read everything, and if the sentences arrive faster than it can count them, it falls behind. With 3 lanes I can start a second consumer and Kafka shares the lanes between them automatically, one consumer taking two lanes and the other taking one. In my Postgres table, from the moment the second consumer joined, every window had two rows, one per instance. Each lane also keeps its own order and its own offsets, which is how Kafka remembers where each reader stopped. Partitions spread the work, and if one consumer stops, Kafka can hand its lanes to the other.
