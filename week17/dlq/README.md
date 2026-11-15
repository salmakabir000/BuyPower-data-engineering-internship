\# Week 17 - Dead Letter Queue (DLQ)



\## Project Overview



This project implements a Dead Letter Queue (DLQ) for handling messages that fail during processing in a Kafka streaming pipeline.



Failed messages are captured and stored so they can be reviewed, replayed, or discarded without stopping the main consumer.



\## Features



\- Captures failed Kafka messages.

\- Sends failed messages to the `sentences.dlq` topic.

\- Stores failed messages and error details in PostgreSQL.

\- Tracks processing attempts.

\- Identifies poison messages.

\- Provides commands to list, show, replay, and discard messages.

\- Includes tests for malformed JSON, missing fields, invalid encoding, and empty messages.



\## Project Structure



```text

week17/

└── dlq/

&#x20;   ├── consumer.py

&#x20;   ├── database.py

&#x20;   ├── dlq.py

&#x20;   ├── requirements.txt

&#x20;   ├── README.md

&#x20;   └── tests/

&#x20;       └── test\_dlq.py

```



\## Requirements



\- Python 3

\- Apache Kafka

\- PostgreSQL



\## Installation



Install dependencies:



```bash

pip install -r requirements.txt

```



\## Environment Variables



Configure the following environment variables:



\- `KAFKA\_BOOTSTRAP\_SERVERS`

\- `PG\_CONN`



\## Database



The application creates a PostgreSQL table named `dlq\_records` to store failed messages, error details, timestamps, and retry information.



\## Running the Consumer



```bash

python consumer.py

```



\## DLQ Management Commands



List failed messages:



```bash

python dlq.py list

```



Show a specific failed message:



```bash

python dlq.py show 1

```



Replay a failed message:



```bash

python dlq.py replay 1

```



Discard a failed message:



```bash

python dlq.py discard 1

```



\## Testing



Run the tests using:



```bash

pytest tests/

```



\## Conclusion



The project demonstrates how a Dead Letter Queue can improve message-processing reliability by isolating failed messages, recording errors, and providing message-management operations.

