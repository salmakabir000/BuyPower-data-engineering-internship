\# Data Lake File Organizer



\## Overview



This project organizes NYC Yellow Taxi Parquet data into a Hive-style data lake using year, month, and day partitions.



\## Partitioning



The data is partitioned as:



data\_lake/trips/year=2024/month=01/day=01/



Year/month/day partitioning was chosen because queries commonly filter by pickup date. This allows a query for a specific date to read only the relevant partition instead of scanning the entire dataset.



\## Ingestion



The `ingest` command reads a Parquet file and organizes the records according to `tpep\_pickup\_datetime`.



Example:



`python lake\_organizer.py ingest yellow\_tripdata\_2024-01.parquet --dataset trips`



\## Compaction



The `compact` command combines multiple Parquet files within a partition when possible, reducing the number of small files.



Example:



`python lake\_organizer.py compact trips --year 2024 --month 01`



\## Late-arriving Data



If overlapping data is ingested again, the current implementation appends the new records. Deduplication would be added in a larger production system where unique trip identifiers are available.



\## Small Files



Small files can reduce query performance because many files must be opened separately. Compaction helps reduce this problem.



\## Scaling



If the dataset became 1000 times larger, additional partitioning and distributed processing would be considered. Partition keys would need to remain selective without creating excessive numbers of small files.



