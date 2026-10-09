# Config-Driven Pipeline

A small pipeline engine where **pipelines are defined in YAML, not Python**.
The engine is generic: it contains no CoinGecko, weather, or any other
source-specific logic. Three very different pipelines run on the same engine
with zero new Python code.

## Usage
```bash
pip install -r requirements.txt

python -m config_pipeline run pipelines/crypto.yml
python -m config_pipeline run pipelines/weather.yml
python -m config_pipeline run pipelines/csv_ingest.yml

# check a config without running it (no network, no file writes)
python -m config_pipeline validate pipelines/csv_ingest.yml
```

## The pipeline format
```yaml
name: crypto_prices
source:
  type: rest_api          # which source component to use
  config: { ... }         # settings for that source
transforms:               # optional, applied in order
  - type: select_columns
    columns: [id, symbol]
destination:
  type: sqlite
  config: { ... }
```
Note: source and destination settings sit under `config:`, while a
transform's settings sit directly beside its `type:` (this follows the
format given in the assignment).

## Built-in components
| Kind | Types |
|---|---|
| Sources | `rest_api`, `csv_file`, `parquet_file` |
| Transforms | `select_columns`, `rename_column`, `add_column`, `filter_rows`, `cast_column` |
| Destinations | `sqlite`, `parquet_file` |

`rest_api` is generic about response shape. `response_format: records` is a
list of row objects (CoinGecko). `response_format: columnar` is a dict of
column -> list of values (Open-Meteo). `response_path` points at the part of
the JSON to use, and optional `pagination` fetches several pages.

## How dynamic loading works
`registry.py` is what makes this config-driven. A source of type
`rest_api` lives at `config_pipeline/sources/rest_api.py` and must define a
class named `Source` (transforms define `Transform`, destinations define
`Destination`). Given only the string from the YAML, the registry builds the
module path, imports it at runtime with `importlib.import_module()`, and
pulls out the class. The engine never imports or names a specific component.

**Adding a new component = adding one file.** No engine changes needed.

## Fail-fast validation
`validate` (and `run`, before doing any work) checks that the YAML parses,
required fields exist, and every `type` resolves to a real component. Errors
name the exact problem, for example an unknown type points at the file that
would need to exist.

## Tradeoffs of config-driven pipelines

**What gets easier**
- A new pipeline is a small YAML file, so no new Python and no new deploy of
  engine code. Someone who doesn't write Python can still define one.
- Pipelines are consistent. They all get the same logging, validation and
  error handling for free.
- Reviews are small. A change to a pipeline is a few lines of YAML diff.
- Mistakes are caught early, since `validate` checks a config before it runs.
- Components are reusable. One `sqlite` destination serves every pipeline.

**What gets harder**
- Anything the config language can't express needs a new component. Joins,
  conditional logic, or custom business rules mean writing Python anyway.
- The YAML format becomes a small language that must be documented and kept
  backward compatible. Changing a field name can break every existing config.
- Debugging is less direct. When a run fails, the error comes from the
  engine, not from a line of your own code, so you have to map it back to
  the YAML.
- Some mistakes only show up at run time. `validate` checks structure and
  types, but it can't know that a column name is wrong until data arrives.
- Inconsistencies in the format (like transforms not using `config:`) are
  easy to introduce and then hard to remove once configs exist.

The sweet spot is many similar pipelines with simple logic. For one-off
pipelines with heavy custom logic, plain Python is often simpler.
