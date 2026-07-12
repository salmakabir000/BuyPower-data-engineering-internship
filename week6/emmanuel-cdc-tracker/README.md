\# CDC Tracker



This project implements a simple polling-based Change Data Capture (CDC) tracker using PostgreSQL and Python.



\## Features

\- Connects to PostgreSQL

\- Reads changes from the customers table

\- Stores the last processed watermark in last\_run.json

\- Writes events to events.jsonl in JSON Lines format



\## Polling CDC vs Debezium



This project uses polling CDC, where the database is queried at regular intervals.



Debezium uses the PostgreSQL transaction log (WAL), making it faster and more reliable.



\## Limitations of Polling CDC



\- Can miss hard deletes

\- Less efficient for high-throughput databases

\- Requires frequent database queries

\- Schema changes are harder to detect

