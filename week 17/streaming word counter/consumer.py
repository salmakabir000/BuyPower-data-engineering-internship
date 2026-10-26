import json
import re
import time
from collections import Counter

from kafka import KafkaConsumer


# Connect to Redpanda and subscribe to the topic
consumer = KafkaConsumer(
    "sentences",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda message: json.loads(
        message.decode("utf-8")
    )
)


word_counts = Counter()

# Track when we last printed the results
last_print_time = time.monotonic()


print("Consumer started. Waiting for sentences...")


try:
    # 3. Read messages continuously
    for message in consumer:

        # Extract the sentence from the JSON message
        sentence = message.value["sentence"]

        # 4. Convert to lowercase and extract words
        words = re.findall(r"\b\w+\b", sentence.lower())

        # 5. Update the word counts
        word_counts.update(words)

        # 6. Print the top 5 words every 10 seconds
        current_time = time.monotonic()

        if current_time - last_print_time >= 10:

            print("\nTop 5 words seen so far:")
            print("------------------------")

            for word, count in word_counts.most_common(5):
                print(f"{word}: {count}")

            print("------------------------")

            last_print_time = current_time


except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:
    consumer.close()