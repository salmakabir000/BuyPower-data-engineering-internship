# Data Lake File Organizer

This project is a simple command-line tool for organizing Parquet files into a partitioned data lake.

The main idea is to organize files by the date of the taxi trip:

data_lake/
└── trips/
    └── year=2024/
        └── month=01/
            └── day=15/
                └── part-001.parquet

This makes it possible to find data for a specific day without scanning the entire dataset.

## Dataset

The project uses the 2024 NYC Yellow Taxi Trip dataset for January through June.

The source files contain millions of taxi trips and include a `tpep_pickup_datetime` column, which is used as the partition key.

## Project Structure

```
data-lake-file-organizer/
├── data/
│   ├── yellow_tripdata_2024-01.parquet
│   ├── yellow_tripdata_2024-02.parquet
│   ├── ...
│   └── yellow_tripdata_2024-06.parquet
├── data_lake/
│   └── trips/
│       └── year=YYYY/
│           └── month=MM/
│               └── day=DD/
│                   └── part-XXX.parquet
├── lake_organize.py
└── README.md
```

## How It Works
### Ingest
```
python lake_organize.py ingest data/yellow_tripdata_2024-01.parquet --dataset trips
```
The ingest command reads a Parquet file and groups the records by tpep_pickup_datetime.
For each date, it creates a Hive-style partition.
The tool estimates how many rows will fit into a roughly 128 MB Parquet file. If a day's data is large enough to require multiple files, it creates:
```
part-001.parquet
part-002.parquet
part-003.parquet
```
If data is ingested again for the same partition, new part files are appended rather than overwriting the existing files.

### Compact
```
python lake_organize.py compact trips --year 2024 --month 01
```
The compact command looks at every day partition within a specified month.
If a day contains multiple Parquet files, they are combined into fewer files.
The number of rows remains the same after compaction.


## Why I Chose Year / Month / Day
I chose year/month/day because the main time-based column in the dataset is tpep_pickup_datetime, and queries are likely to filter trips by date or date range.
For example, if I only need trips from January 15, 2024, I can read:
```
data_lake/trips/year=2024/month=01/day=15/
```
instead of scanning all six months of data.
Date is also a natural partition key because the data is time-based.


## What If the Dataset Were 1000x Larger?
If the dataset became much larger, some days would contain a lot more data.
Instead of one file per day, a day might need several files:
```
day=15/
├── part-001.parquet
├── part-002.parquet
└── part-003.parquet
```
The same basic idea would still work, but I would need to think more carefully about file sizes and how the data is processed.
For a much larger dataset, I would probably use a tool like Spark instead of loading everything into pandas at once.

## Technologies
Python
Pandas
PyArrow
Parquet
argparse
Linux
