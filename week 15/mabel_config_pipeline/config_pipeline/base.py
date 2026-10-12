"""Shared building blocks: errors, the Field schema helper, and the Component base class."""
from __future__ import annotations

import copy
import difflib
from dataclasses import dataclass
from typing import Any


class ConfigError(Exception):
    """The YAML file is wrong. Carries every problem found, not just the first."""

    def __init__(self, errors):
        if isinstance(errors, str):
            errors = [errors]
        self.errors = list(errors)
        super().__init__("\n".join(self.errors))


class PipelineError(Exception):
    """The config was fine, but something went wrong while running."""


@dataclass(frozen=True)
class Field:
    types: tuple
    required: bool = False
    default: Any = None
    choices: tuple | None = None


def field(*types, required=False, default=None, choices=None) -> Field:
    """Declare one option a component accepts. Use `object` to accept anything."""
    return Field(tuple(types), required, default, choices)


def _type_names(types) -> str:
    return " or ".join("any value" if t is object else t.__name__ for t in types)


def validate_fields(schema: dict[str, Field], config: Any, path: str) -> list[str]:
    """Check a config mapping against a schema. Returns a list of readable errors."""
    if not isinstance(config, dict):
        return [f"{path}: expected a mapping, got {type(config).__name__}"]

    errors: list[str] = []
    for key in config:
        if key not in schema:
            msg = f"{path}.{key}: unknown option"
            close = difflib.get_close_matches(key, schema, n=1)
            if close:
                msg += f" (did you mean '{close[0]}'?)"
            elif schema:
                msg += f" (allowed: {', '.join(sorted(schema))})"
            errors.append(msg)

    for key, f in schema.items():
        value = config.get(key)
        if value is None:
            if f.required:
                errors.append(f"{path}.{key}: required")
            continue
        if object not in f.types:
            is_bool = isinstance(value, bool)
            ok = isinstance(value, f.types) and (bool in f.types or not is_bool)
            if not ok:
                errors.append(
                    f"{path}.{key}: expected {_type_names(f.types)}, got {type(value).__name__}"
                )
                continue
        if f.choices and value not in f.choices:
            errors.append(f"{path}.{key}: must be one of {list(f.choices)}, got {value!r}")
    return errors


class Component:
    """Base class for every source, transform and destination."""

    schema: dict[str, Field] = {}

    @classmethod
    def check(cls, config: dict, path: str) -> list[str]:
        """Extra validation beyond the schema. Override when needed."""
        return []

    def __init__(self, config: dict):
        self.config = {k: copy.deepcopy(f.default) for k, f in self.schema.items()}
        self.config.update({k: v for k, v in (config or {}).items() if v is not None})
