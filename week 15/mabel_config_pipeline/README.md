# config_pipeline

One generic engine. Many YAML pipelines. The engine has no idea what CoinGecko or a weather API is; it only knows how to read a card (YAML) and run **source → transforms → destination**.

```
config_pipeline run pipelines/crypto.yml
config_pipeline run pipelines/weather.yml
config_pipeline validate pipelines/csv_ingest.yml   # checks the config, runs nothing
```

## Install

```
pip install -e ".[dev]"
pytest
```

## How it works

| File | Job |
|---|---|
| `engine.py` | Load YAML → render templates → validate → build components → run |
| `registry.py` | Maps `type: rest_api` to a class and loads it with `importlib` |
| `base.py` | `Field` schema helper, `Component` base class, error types |
| `sources.py` | `rest_api`, `csv_file`, `parquet_file` |
| `transforms.py` | `select_columns`, `rename_column`, `add_column`, `filter_rows`, `cast_column` |
| `destinations.py` | `sqlite`, `parquet_file` |
| `template.py` | `{{ now() }}`, `{{ today() }}`, `{{ env.NAME }}` |

Every component declares its options in a `schema`. The validator reads that schema to check a config **without creating the component or touching any data**, which is how `validate` works.

### Plugging in your own component

`type` also accepts a dotted path, so you never have to edit the engine:

```yaml
source:
  type: my_package.my_module:MySource
```

## Pipeline file format

```yaml
name: my_pipeline
source:
  type: <source type>
  config: { ... }
transforms:                    # optional; options sit beside `type`
  - type: <transform type>
    ...
destination:
  type: <destination type>
  config: { ... }
```

### Options

- **rest_api**: `base_url`*, `endpoint`, `params`, `headers`, `records_path` (dotted path to the data in the JSON), `pagination` (`type: page`, `param_name`, `max_pages`, `start_page`), `timeout`, `retries`. Handles both lists of records and columnar JSON (`{"col": [...]}`). Pagination stops early on an empty page.
- **csv_file**: `path`*, `delimiter`, `encoding`
- **parquet_file** (source): `path`*, `columns`
- **select_columns**: `columns`* · **rename_column**: `mapping`* · **add_column**: `name`*, `value`*
- **filter_rows**: `column`*, `op`* (`eq ne gt gte lt lte in not_null`), `value`
- **cast_column**: `column`*, `dtype`* (`int float str bool datetime`)
- **sqlite**: `path`*, `table`*, `primary_key`, `on_conflict` (`fail` / `ignore` / `replace`)
- **parquet_file** (destination): `path`*, `mode` (`overwrite` / `append`), `primary_key` (dedupes, keeping the newest)

`*` = required.

## Errors

- Invalid config → **all** problems are listed at once, with "did you mean…" hints, exit code `2`.
- Valid config but the run fails (API down, file missing) → exit code `1`.
- Success → exit code `0`.

```
✗ bad.yml is invalid (3 problems):
  - source.type: unknown source type 'rest_apii' (did you mean 'rest_api'?)
  - destination.config.tabel: unknown option (did you mean 'table'?)
  - destination.config.table: required
```

## Tradeoffs of config-driven pipelines

**What gets easier**
- Adding a source is a 20-line YAML file, not a new Python program.
- Fix a bug once in the engine and every pipeline gets the fix.
- Non-Python teammates can add or change pipelines, and reviewers can read a config diff in seconds.
- Configs can be validated *before* a run, so typos fail fast instead of halfway through.
- Consistent logging, retries and error handling for free.

**What gets harder**
- Debugging: the logic is spread across the YAML, the registry and the engine, so a stack trace points at the engine rather than at "your" pipeline.
- YAML is a weak programming language. Conditionals, loops and custom logic get awkward fast, and people end up inventing a language inside YAML.
- The engine becomes a product: you maintain its schema, docs and versioning, and changing a transform's options can break every pipeline that uses it.
- Anything outside the built-in components needs a plugin class anyway.
- Typos in *values* (not keys) are still only caught at run time, e.g. a column name that does not exist in the data.
- Less flexible than code for one-off, unusual pipelines. Config-driven shines when you have many similar pipelines, and is overkill for one or two.

## Stretch goal: scheduler

The whole job of a scheduled task is one call:

```python
from config_pipeline.engine import run_file
run_file("pipelines/crypto.yml")
```

or `config_pipeline run pipelines/crypto.yml` from a BashOperator.

## Note on the sample pipelines

`crypto.yml` and `weather.yml` need internet access. The test suite runs the same code paths against a local mock HTTP server so it works offline.
