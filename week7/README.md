# Week 7 – GitHub Archive Data Loading

## Project Overview

This project loads GitHub Archive event data into a PostgreSQL data warehouse using a star schema developed in Week 6.

The pipeline processes hourly compressed JSON files, extracts dimension and fact records, resolves surrogate keys, and loads the data into PostgreSQL.

## Architecture

**Source:** GitHub Archive hourly event files (JSON.gz)

**Destination:** PostgreSQL (`github_warehouse`)

**Process:**

1. Read compressed JSON event files.
2. Extract event types, actors, and repositories.
3. Load dimension records into PostgreSQL.
4. Retrieve surrogate key mappings from dimensions.
5. Resolve dimension keys for each event.
6. Load event records into the fact table.

## Data Source

[GitHub Archive](https://www.gharchive.org/)

Dataset processed: January 1–3, 2024

- 3 days of data
- 24 hourly files per day
- 72 compressed JSON files total

Raw data is stored locally in `week7/data/raw/` and excluded from Git because of its large size.

## Data Warehouse Schema

The pipeline loads data into the Week 6 star schema.

| Table | Description |
|---|---|
| `dim_actor` | GitHub users associated with events |
| `dim_repo` | GitHub repositories associated with events |
| `dim_event_type` | Types of GitHub events |
| `dim_date` | Date attributes for analytical queries |
| `fact_events` | Central event fact table |

The fact table references dimensions using surrogate keys.

## Technologies

- Python 3
- PostgreSQL
- psycopg2
- gzip
- JSON
- SQL

## Running the Pipeline

### 1. Install dependencies

```bash
pip install psycopg2-binary
```

### 2. Configure database credentials

Set the database password as an environment variable:

```bash
export DB_PASSWORD='your_password'
```

### 3. Ensure the database schema is ready

Create the database and apply the Week 6 schema and required dimension setup before loading the data.

### 4. Download the raw data

Hourly files follow this naming pattern:

`YYYY-MM-DD-H.json.gz`

Download files from:

`https://data.gharchive.org/`

Place the downloaded files in:

`week7/data/raw/`

### 5. Run the loader

From the repository root:

```bash
python week7/loader.py
```

The loader processes each hourly file, loads dimension records, resolves surrogate keys, and inserts events into the fact table.

## Data Quality and Validation

The pipeline uses `ON CONFLICT DO NOTHING` to prevent duplicate dimension and fact records when data is reprocessed.

Validation checks included:

- Total event records loaded
- Duplicate event IDs
- Daily event counts
- Event distribution by type
- Foreign key references to unknown dimension records

## Results

| Metric | Result |
|---|---:|
| Files processed | 72 |
| Total events extracted | 12,999,008 |
| Events loaded | 12,998,995 |
| Duplicate events skipped | 13 |
| Unique events in fact table | 12,998,995 |
| Unknown actor references | 0 |
| Unknown repository references | 0 |
| Unknown event type references | 0 |

### Daily Event Counts

| Date | Events |
|---|---:|
| 2024-01-01 | 3,879,837 |
| 2024-01-02 | 4,304,461 |
| 2024-01-03 | 4,814,697 |
| **Total** | **12,998,995** |

## Key Features

- Batch loading with `execute_values`
- Dimension loading before fact loading
- Surrogate key lookups
- Duplicate prevention through database constraints
- Idempotent loading
- Compressed file processing
- Multi-file processing

## Outcome

Successfully processed 72 hourly GitHub Archive files and loaded approximately 13 million unique events into a PostgreSQL star schema for analytical querying.
