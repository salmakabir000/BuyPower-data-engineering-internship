# Streaming Word Counter

## About the Project

This project reads text from *Alice's Adventures in Wonderland* and counts how often each word appears in 30-second windows.

I used Redpanda to stream the text, Python to process the messages, and PostgreSQL to store the word counts.

## How It Works

1. `producer.py` downloads the book from Project Gutenberg, splits the text into smaller segments, and sends them to the `sentences` topic.
2. Redpanda distributes the messages across three partitions.
3. `consumer.py` reads the messages, converts the text to lowercase, extracts the words, and counts their occurrences.
4. PostgreSQL stores the counts for each word and its corresponding 30-second window.

Each message includes the text and a UTC timestamp.

## Tools Used

- Python
- Redpanda
- PostgreSQL
- Docker
- `kafka-python`
- `psycopg`

## Running the Project

Make sure the Redpanda and PostgreSQL containers are running, and that the `sentences` topic and database tables have been created.

Activate the virtual environment and install the dependencies:

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

Start the consumer in one terminal:

```bash
python consumer.py
```

Start the producer in another terminal:

```bash
python producer.py
```

The producer sends the text segments with a short, randomized delay between messages.

## 30-Second Windows

The word counts are grouped into fixed 30-second windows using the timestamp attached to each message.

The `word_counts_30s` table stores the window start, window end, word, and count. Its primary key is the combination of `window_start` and `word`.

To see the top 10 words in each window, run:

```sql
SELECT window_start, word, count
FROM (
    SELECT
        window_start,
        word,
        count,
        ROW_NUMBER() OVER (
            PARTITION BY window_start
            ORDER BY count DESC, word ASC
        ) AS rank
    FROM word_counts_30s
) ranked
WHERE rank <= 10
ORDER BY window_start DESC, rank;
```

## Testing

I tested the pipeline with three sample messages:

- `Rabbit rabbit meets Alice`
- `Alice reads a book`
- `Rabbit runs quickly`

The resulting counts included three occurrences of `rabbit`, two of `alice`, and one of `book`.

I also ran two consumers in the same consumer group. Redpanda distributed the three partitions between them, and the group reported zero lag during the test.

## What I Learned

This project helped me practise streaming messages, working with Kafka-compatible topics and partitions, grouping events into time windows, and storing aggregated results in PostgreSQL.

There are still areas I would improve, particularly handling late-arriving messages and automatically displaying the top 10 words when a window closes.

### Dead Letter Queue Extension

Each record stores:

- Original message as binary data (`BYTEA`)
- Error type and error message
- Consumer name and failure timestamp
- Original topic, partition, and offset
- Initial attempt count and replay count
- Current status

### DLQ Command-Line Interface

List recent failed messages:
python dlq.py list --limit 20


Inspect a specific message:
python dlq.py show 5


Replay a message:
python dlq.py replay 5


Replay pending messages from the last hour:
python dlq.py replay --all --since 1h


Discard a pending message:
python dlq.py discard 6


Testing
The DLQ was tested with:
- Malformed JSON
- Messages missing the required sentence field
- Invalid UTF-8 bytes
- Repeated replay failures
- Manual message discard
The tests confirmed that failed messages were published to the DLQ, persisted in PostgreSQL, and could be inspected and managed through the CLI.
Monitoring Failed Messages
To see the number of failures grouped by error type:
SELECT
    error_type,
    COUNT(*) AS failed_messages
FROM dlq_records
GROUP BY error_type
ORDER BY failed_messages DESC;


What I Learned
This extension helped me practise dead-letter queue patterns, preserving malformed messages, replay management, poison-message handling, retry strategies, and operational monitoring in a streaming pipeline.
EOF
