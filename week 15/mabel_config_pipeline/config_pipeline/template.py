"""Tiny template language for YAML values: {{ now() }}, {{ today() }}, {{ env.NAME }}."""
from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any

from .base import ConfigError

_EXPR = re.compile(r"\{\{\s*(.+?)\s*\}\}")
_ENV = re.compile(r"^env\.([A-Za-z_][A-Za-z0-9_]*)$")


def make_context() -> dict[str, str]:
    """One timestamp per run, so every row in a run shares the same `now()`."""
    now = datetime.now(timezone.utc)
    return {"now": now.isoformat(timespec="seconds"), "today": now.date().isoformat()}


def _eval(expr: str, ctx: dict, strict_env: bool, path: str) -> str:
    if expr == "now()":
        return ctx["now"]
    if expr == "today()":
        return ctx["today"]
    m = _ENV.match(expr)
    if m:
        name = m.group(1)
        if name in os.environ:
            return os.environ[name]
        if strict_env:
            raise ConfigError(f"{path}: environment variable {name} is not set")
        return ""
    raise ConfigError(
        f"{path}: unknown template expression '{{{{ {expr} }}}}' "
        "(allowed: now(), today(), env.NAME)"
    )


def render(obj: Any, ctx: dict, strict_env: bool = True, path: str = "$") -> Any:
    """Walk the config and replace template expressions in every string."""
    if isinstance(obj, str):
        return _EXPR.sub(lambda m: _eval(m.group(1), ctx, strict_env, path), obj)
    if isinstance(obj, list):
        return [render(v, ctx, strict_env, f"{path}[{i}]") for i, v in enumerate(obj)]
    if isinstance(obj, dict):
        return {k: render(v, ctx, strict_env, f"{path}.{k}") for k, v in obj.items()}
    return obj
