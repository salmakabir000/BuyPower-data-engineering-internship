"""The generic engine. It knows nothing about any specific API, table or file."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml

from . import registry
from .base import ConfigError, PipelineError, field, validate_fields
from .template import make_context, render

log = logging.getLogger("config_pipeline")

TOP_LEVEL = {
    "name": field(str, required=True),
    "source": field(dict, required=True),
    "transforms": field(list, default=[]),
    "destination": field(dict, required=True),
}
COMPONENT = {"type": field(str, required=True), "config": field(dict, default={})}
TRANSFORM = {"type": field(str, required=True)}  # transform options live next to `type`


def load_yaml(path: str | Path) -> dict:
    """Read a YAML file. Syntax errors become ConfigErrors with line numbers."""
    try:
        text = Path(path).read_text()
    except FileNotFoundError:
        raise ConfigError(f"file not found: {path}")
    try:
        data = yaml.safe_load(text)
    except yaml.MarkedYAMLError as exc:
        mark = exc.problem_mark
        where = f" (line {mark.line + 1}, column {mark.column + 1})" if mark else ""
        raise ConfigError(f"invalid YAML{where}: {exc.problem}")
    if not isinstance(data, dict):
        raise ConfigError("top level of the file must be a mapping with name/source/destination")
    return data


def _component_errors(kind: str, raw: Any, path: str, flat: bool = False):
    """Validate one source/transform/destination block. Returns (class | None, errors)."""
    if not isinstance(raw, dict):
        return None, [f"{path}: expected a mapping, got {type(raw).__name__}"]
    type_name = raw.get("type")
    if not isinstance(type_name, str):
        return None, [f"{path}.type: required (a string)"]
    try:
        cls = registry.resolve(kind, type_name)
    except LookupError as exc:
        return None, [f"{path}.type: {exc}"]

    if flat:  # transforms: options sit beside `type`
        cfg = {k: v for k, v in raw.items() if k != "type"}
        base = path
    else:  # sources/destinations: options sit under `config`
        errors = validate_fields(COMPONENT, raw, path)
        if errors:
            return cls, errors
        cfg = raw.get("config") or {}
        base = f"{path}.config"
    errors = validate_fields(cls.schema, cfg, base)
    if not errors:
        errors = cls.check(cfg, base)
    return cls, errors


def check(raw: dict, ctx: dict | None = None, strict_env: bool = False):
    """Validate a parsed pipeline. Returns (rendered_config, errors) and never runs anything."""
    try:
        rendered = render(raw, ctx or make_context(), strict_env=strict_env)
    except ConfigError as exc:
        return raw, exc.errors

    errors = validate_fields(TOP_LEVEL, rendered, "pipeline")
    if errors:
        return rendered, errors

    _, e = _component_errors("source", rendered["source"], "source")
    errors += e
    for i, t in enumerate(rendered.get("transforms") or []):
        _, e = _component_errors("transform", t, f"transforms[{i}]", flat=True)
        errors += e
    _, e = _component_errors("destination", rendered["destination"], "destination")
    errors += e
    return rendered, errors


def validate_file(path: str | Path) -> dict:
    """Validate without running. Raises ConfigError listing every problem."""
    rendered, errors = check(load_yaml(path))
    if errors:
        raise ConfigError(errors)
    return rendered


def run_file(path: str | Path) -> dict:
    """Validate, then run: source -> transforms -> destination."""
    raw = load_yaml(path)
    rendered, errors = check(raw, strict_env=True)
    if errors:
        raise ConfigError(errors)

    src_cls = registry.resolve("source", rendered["source"]["type"])
    source = src_cls(rendered["source"].get("config"))
    transforms = []
    for t in rendered.get("transforms") or []:
        cls = registry.resolve("transform", t["type"])
        transforms.append((t["type"], cls({k: v for k, v in t.items() if k != "type"})))
    dst_cls = registry.resolve("destination", rendered["destination"]["type"])
    destination = dst_cls(rendered["destination"].get("config"))

    name = rendered["name"]
    log.info("[%s] reading from %s", name, rendered["source"]["type"])
    df = source.read()
    rows_read = len(df)
    log.info("[%s] read %d rows", name, rows_read)

    for type_name, transform in transforms:
        df = transform.apply(df)
        log.info("[%s] %s -> %d rows, %d columns", name, type_name, len(df), len(df.columns))

    written = destination.write(df)
    log.info("[%s] wrote %d rows to %s", name, written, rendered["destination"]["type"])
    return {"pipeline": name, "rows_read": rows_read, "rows_out": len(df), "rows_written": written}
