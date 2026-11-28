# Mini dbt

A SQL transformation tool built with Python and PostgreSQL, inspired by dbt.

The idea was to build a tool that can find SQL models in a project, understand how they depend on one another, and run them in the correct order.

I used my GitHub analytics warehouse as the example project.

## What it does

- Finds SQL models in the project directory.
- Extracts `ref()` and `source()` references from SQL files.
- Checks that model references exist and detects circular dependencies.
- Determines the order in which models should run.
- Runs SQL transformations in PostgreSQL.
- Supports creating tables and views.
- Allows individual models and their dependencies to be selected for execution.
- Generates an HTML page showing model details, dependencies, column descriptions and SQL.

## Project structure

```text
mini-dbt/
├── mini_dbt/
│   ├── cli.py
│   ├── config.py
│   ├── dag.py
│   ├── docs.py
│   ├── parser.py
│   └── runner.py
├── example_project/
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   └── project.yml
├── tests/
├── profiles.yml.example
├── .gitignore
└── README.md
```

## Example project

The example uses a GitHub analytics warehouse with seven SQL models.

**Staging models**
- `stg_actors`
- `stg_dates`
- `stg_event_types`
- `stg_events`
- `stg_repos`

**Mart models**
- `mart_event_details`
- `mart_daily_event_counts`

The staging models read from existing warehouse tables, while the mart models combine and aggregate the staging data.

## How to run

### 1. Install dependencies

```bash
pip install pyyaml psycopg2-binary pytest
```

### 2. Set up the database profile

Copy the example profile:

```bash
cp profiles.yml.example profiles.yml
```

Update it with your PostgreSQL connection details.

Set the database password as an environment variable. For example:

```bash
export DB_PASSWORD="your_database_password"
```

### 3. Run the commands

Run from the `mini-dbt` directory.

**Check the project and model execution order:**

```bash
PYTHONPATH=. python -m mini_dbt.cli compile
```

**Run all models:**

```bash
PYTHONPATH=. python -m mini_dbt.cli run
```

**Run a specific model:**

```bash
PYTHONPATH=. python -m mini_dbt.cli run --select stg_event_types
```

**Run a model and its downstream models:**

```bash
PYTHONPATH=. python -m mini_dbt.cli run --select stg_events+
```

**Generate HTML documentation:**

```bash
PYTHONPATH=. python -m mini_dbt.cli docs
```

The documentation is saved to `target/index.html`.

## Tests

Run the tests with:

```bash
PYTHONPATH=. python -m pytest -v
```

The test suite covers configuration, SQL parsing, dependency handling, model selection, SQL rendering, materialization and documentation generation.

Latest test result: **21 passed**.

## Technologies used

Python, PostgreSQL, SQL, YAML and pytest.
