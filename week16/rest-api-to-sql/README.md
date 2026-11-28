# REST API to SQL Bridge

A Python pipeline that fetches issues and pull requests from the Apache Airflow GitHub repository and loads them into PostgreSQL.

## Features

- YAML-based configuration
- Bearer token authentication
- Link-header, cursor and page-number pagination
- API retries and rate limiting
- Full refresh and incremental sync
- JSON flattening and PostgreSQL upserts
- Checkpoint tracking

## Tech Stack

Python · PostgreSQL · Requests · PyYAML · Psycopg 3

## Setup

```bash
git clone https://github.com/salmakabir000/BuyPower-data-engineering-internship.git
cd BuyPower-data-engineering-internship/week16/rest-api-to-sql

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Set the required environment variables:

```bash
export GITHUB_TOKEN="your_github_token"
export PG_CONN="postgresql://username:password@localhost:5432/database_name"
```

Create the `raw_github_issues` and `pipeline_state` tables in PostgreSQL using the schemas in this project.

## Run

Incremental sync:

```bash
python api2sql.py run configs/github_issues.yml
```

Full refresh:

```bash
python api2sql.py run configs/github_issues.yml --full-refresh
```

Check pipeline status:

```bash
python api2sql.py status
```

## Data

The pipeline extracts fields such as issue ID, number, title, state, author and timestamps. The original API response is preserved in a JSONB column.

Existing records are updated using PostgreSQL upserts. Incremental runs use the saved `updated_at` checkpoint.

## Results

Successfully ingested **2,122 records** from the Apache Airflow repository.

Tested full refresh, incremental sync, upserts, checkpoint tracking and all three pagination strategies.
