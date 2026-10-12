"""Name -> class lookup. Classes are loaded lazily with importlib, by name."""
from __future__ import annotations

import difflib
import importlib

BUILTIN: dict[str, dict[str, str]] = {
    "source": {
        "rest_api": "config_pipeline.sources:RestApiSource",
        "csv_file": "config_pipeline.sources:CsvFileSource",
        "parquet_file": "config_pipeline.sources:ParquetFileSource",
    },
    "transform": {
        "select_columns": "config_pipeline.transforms:SelectColumns",
        "rename_column": "config_pipeline.transforms:RenameColumn",
        "add_column": "config_pipeline.transforms:AddColumn",
        "filter_rows": "config_pipeline.transforms:FilterRows",
        "cast_column": "config_pipeline.transforms:CastColumn",
    },
    "destination": {
        "sqlite": "config_pipeline.destinations:SqliteDestination",
        "parquet_file": "config_pipeline.destinations:ParquetFileDestination",
    },
}


def resolve(kind: str, type_name: str):
    """Turn a `type:` string into a class.

    `type` is either a built-in name ("rest_api") or a dotted path to your own
    class ("my_pkg.my_module:MySource"), so users can plug in new components
    without touching the engine.
    """
    if ":" in type_name:
        module_path, _, class_name = type_name.partition(":")
        try:
            return getattr(importlib.import_module(module_path), class_name)
        except (ImportError, AttributeError) as exc:
            raise LookupError(f"cannot import {type_name!r}: {exc}") from exc

    known = BUILTIN[kind]
    if type_name not in known:
        msg = f"unknown {kind} type {type_name!r}"
        close = difflib.get_close_matches(type_name, known, n=1)
        msg += f" (did you mean '{close[0]}'?)" if close else f" (available: {', '.join(known)})"
        raise LookupError(msg)

    module_path, _, class_name = known[type_name].partition(":")
    return getattr(importlib.import_module(module_path), class_name)
