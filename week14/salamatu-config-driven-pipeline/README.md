# Config-Driven Data Pipeline

## About the Project

This project is a Python pipeline engine that uses YAML files to define how data should be extracted, transformed, and loaded.

The idea is to use the same engine for different pipelines instead of writing a separate Python script for each one. To change a pipeline, I can update its configuration without changing the engine itself.

## What It Supports

**Sources**
- CSV files
- Parquet files
- REST APIs

**Transformations**
- Select columns
- Rename columns
- Add columns
- Filter rows
- Cast column data types

**Destinations**
- SQLite
- Parquet files

## Project Structure

```text
components/
  sources/
  transforms/
  destinations/
pipelines/
data/
tests/
config_pipeline.py
requirements.txt
README.md
```

## Setup

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
python -m pip install -r requirements.txt
```

## Running a Pipeline

Validate a configuration before running it:

```bash
python config_pipeline.py validate pipelines/customers.yml
```

Run a pipeline:

```bash
python config_pipeline.py run pipelines/customers.yml
```

Other available configurations:

```bash
python config_pipeline.py run pipelines/crypto.yml
python config_pipeline.py run pipelines/weather.yml
```

## Example Pipelines

- **Customers:** Reads a CSV file, filters and transforms customer records, then saves the result to SQLite.
- **Crypto:** Fetches cryptocurrency market data from CoinGecko, selects the required columns, and saves the result to SQLite.
- **Weather:** Fetches hourly weather data from Open-Meteo, renames columns, adds a source identifier, and saves the result as Parquet.

## Things to Note

- The pipeline uses pandas DataFrames to pass data between components. This keeps the code simple, but loading everything into memory may not work well for very large datasets.
- The SQLite destination replaces the existing table on each run. This is convenient for the current examples, but a production pipeline may need to append records or load incrementally.
- Configuration validation catches common configuration errors, but some problems, such as missing columns or incompatible data values, may only appear during execution.
- The REST API source depends on the external API being available and returning data in the expected format.

## Future Improvements

- Add automated tests.
- Improve error handling and logging.
- Support incremental loading and additional components.
