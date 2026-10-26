import json
import requests
import time
import random
import re
from kafka import KafkaProducer
from datetime import datetime, timezone



#download book
BOOK_URL = "https://www.gutenberg.org/files/84/84-0.txt"

response = requests.get(BOOK_URL, timeout=30)
response.raise_for_status()

book_text = response.text 

#split the book into sentences 
sentences = re.split(r'(?<=[.!?])\s+', book_text)

print(f"Found {len(sentences)} sentence fragments.")


#connect to kafka 
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

print ("connected to kafka")
#Stream sentences to Kafka
try:
    for sentence in sentences:

        sentence = sentence.strip()

        if not sentence:
            continue

        # Generate a UTC timestamp
        timestamp = datetime.now(timezone.utc).isoformat()

        # Create the JSON message
        message = {
            "sentence": sentence,
            "ts": timestamp
        }

        # Send the message to Kafka
        producer.send("sentences", value=message)

        print(f"Sent: {sentence[:80]}")

        # Simulate real-time streaming
        time.sleep(random.uniform(0.1, 1.0))

    # Wait for pending messages to be delivered
    producer.flush()

finally:
    producer.close()
print("Finished streaming the book!")