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
