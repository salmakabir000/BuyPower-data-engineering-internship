Mini Orchestrator and Config-Driven Pipeline

This project is a simple Python-based workflow orchestrator that runs tasks in the correct dependency order. I used a DAG structure where each task can depend on another task. The runner uses topological sorting so tasks such as extract, transform, and load are executed in the correct order.



Task states are tracked as pending, running, success, failed, or skipped. If a task fails, its downstream tasks are skipped. Run history and task history are stored in SQLite so previous DAG runs can be checked later using the status and logs commands.



The project also includes a command-line interface with commands for listing DAGs, running a DAG, checking run history, and viewing task logs. I connected a simple crypto ETL workflow with extract, transform, and load tasks and tested it successfully through the orchestrator.

Config-Driven Pipeline

The project was extended to support config-driven data pipelines. Instead of writing separate Python code for every data source, the pipeline is defined in a YAML configuration file.



The Python engine loads the source, transforms and destination dynamically using importlib. This allows the same engine to run different pipelines without adding source-specific code.

Pipeline Components

Built-in sources include:



REST API

CSV file

Parquet file



Built-in transformations include:



Select columns

Rename columns

Add columns

Filter rows

Cast columns



Built-in destinations include:



SQLite

Parquet

Pipelines Created

Three pipelines were created using YAML configuration files:



pipelines/crypto.yml

Gets cryptocurrency data from the CoinGecko API.

Applies column selection and adds an ingestion timestamp.

Saves the data to SQLite.

pipelines/weather.yml

Gets weather data from the Open-Meteo API.

Selects the required weather fields.

Saves the result to Parquet.

pipelines/csv_ingest.yml

Reads data from a CSV file.

Selects the required columns.

Saves the data to SQLite.

Running a Pipeline

Example:

python .\config_pipeline.py run .\pipelines\crypto.yml


Weather:

python .\config_pipeline.py run .\pipelines\weather.yml


CSV:

python .\config_pipeline.py run .\pipelines\csv_ingest.yml


Validating a Configuration

A configuration can be checked without running the pipeline:

python .\config_pipeline.py validate .\pipelines\crypto.yml


The validation checks required configuration fields and reports configuration errors before the pipeline runs.

Config-Driven Pipeline Benefits

A config-driven approach makes it easier to create new pipelines because the engine does not need to be changed for every new data source. It also keeps pipeline settings separate from the Python code.



One trade-off is that YAML configuration can become more difficult to manage when pipelines become very large or complex. The configuration also needs good validation so errors can be detected before the pipeline starts.

Testing

The three pipelines were tested successfully:



CoinGecko pipeline saved 300 rows to SQLite.

Weather pipeline saved 1 row to weather.parquet.

CSV pipeline saved 3 rows to SQLite.

An invalid configuration was also tested and correctly returned a clear error when the source type was missing.