# Streaming Word Counter

Streams a book through Redpanda (Kafka-compatible) and counts words live.

## Setup
Start Redpanda and create the topic with 3 partitions:

    sudo docker run -d --name=redpanda -p 9092:9092 -p 9644:9644 \
      docker.redpanda.com/redpandadata/redpanda:latest \
      redpanda start --overprovisioned --smp 1 --memory 1G --reserve-memory 0M \
      --node-id 0 --check=false --kafka-addr=PLAINTEXT://0.0.0.0:9092 \
      --advertise-kafka-addr=PLAINTEXT://localhost:9092
    sudo docker exec -it redpanda rpk topic create sentences -p 3

Install the client and download the book:

    pip install kafka-python
    curl -o frankenstein.txt https://www.gutenberg.org/cache/epub/84/pg84.txt

## Run (two terminals)

    python3 consumer.py      # terminal 1: live top-5 word counts every 10 seconds
    python3 producer.py      # terminal 2: streams the book, sentence by sentence

## Status
Basic producer and consumer are done. Tumbling-window aggregation, Postgres
storage and the multi-consumer group are next.
