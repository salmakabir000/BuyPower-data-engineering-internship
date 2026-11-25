
import re
from pathlib import Path


REF_PATTERN = re.compile(
    r"\{\{\s*ref\(['\"]([\w_]+)['\"]\)\s*\}\}"
)

SOURCE_PATTERN = re.compile(
    r"\{\{\s*source\(['\"]([\w_]+)['\"],\s*['\"]([\w_]+)['\"]\)\s*\}\}"
)

COLUMN_PATTERN = re.compile(
    r"--\s*@column\s+(\w+):\s*(.+)"
)

def extract_refs(sql):
    return REF_PATTERN.findall(sql)


def extract_sources(sql):
    return SOURCE_PATTERN.findall(sql)

def extract_columns(sql):
    columns = {}

    for name, description in COLUMN_PATTERN.findall(sql):
        columns[name] = description.strip()

    return columns

def parse_model(file_path):
    sql = Path(file_path).read_text()

    return {
        "name": Path(file_path).stem,
        "sql": sql,
        "refs": extract_refs(sql),
        "sources": extract_sources(sql),
        "columns": extract_columns(sql),
    }

def discover_models(models_path, default_materialization="table"):
    models = []

    for file_path in sorted(Path(models_path).rglob("*.sql")):
        model = parse_model(file_path)
        model["materialized"] = default_materialization
        models.append(model)

    # Check for duplicate model names.
    names = [model["name"] for model in models]

    if len(names) != len(set(names)):
        raise ValueError("Duplicate model names detected")

    return models
