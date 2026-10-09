"""
registry.py - this is the piece that makes the whole engine "config-driven"
instead of hardcoded.

Convention: a source of type "rest_api" lives at
  config_pipeline/sources/rest_api.py
and that file must define a class literally named `Source`.

Given just the string "rest_api" (read straight out of a YAML file), we:
  1. Compute the module path: config_pipeline.sources.rest_api
  2. Import it at runtime with importlib.import_module() - this is the
     "dynamic class loading" the engine uses instead of writing
     `from config_pipeline.sources.rest_api import Source` by hand
  3. Pull out the class named `Source` from that module
  4. Hand it back so the engine can instantiate it

The engine itself never mentions "rest_api", "csv_file", "sqlite", etc. by
name anywhere in its code - it only ever sees whatever string happens to be
in the YAML's `type:` field. That's what lets a brand new source/transform/
destination be added later just by dropping a new file in the right folder,
with zero changes to engine.py.
"""

import importlib

# kind -> (package folder name, fixed class name every module in it must define)
_KINDS = {
    "source": ("sources", "Source"),
    "transform": ("transforms", "Transform"),
    "destination": ("destinations", "Destination"),
}


class ConfigError(ValueError):
    """Raised for anything wrong with a pipeline's YAML - unknown component
    types, missing required fields, etc. Always carries a clear, specific
    message, since the whole point is failing fast with useful errors."""
    pass


def load_component_class(kind, type_name):
    """kind is 'source', 'transform', or 'destination'. type_name is the
    string from the YAML's `type:` field, e.g. 'rest_api'. Returns the class
    - does not instantiate it."""
    if kind not in _KINDS:
        raise ConfigError(f"Internal error: unknown component kind '{kind}'")

    folder, class_name = _KINDS[kind]
    module_path = f"config_pipeline.{folder}.{type_name}"

    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError as e:
        raise ConfigError(
            f"Unknown {kind} type '{type_name}' - no file found at "
            f"config_pipeline/{folder}/{type_name}.py. "
            f"Check for a typo, or add a new {kind} by creating that file."
        ) from e

    if not hasattr(module, class_name):
        raise ConfigError(
            f"config_pipeline/{folder}/{type_name}.py exists, but doesn't "
            f"define a class named '{class_name}'. Every {kind} module must "
            f"expose its logic through a class with that exact name."
        )

    return getattr(module, class_name)


def build_component(kind, type_name, config):
    """Loads the class for (kind, type_name) and instantiates it with config."""
    cls = load_component_class(kind, type_name)
    try:
        return cls(config or {})
    except Exception as e:
        raise ConfigError(
            f"Failed to set up {kind} '{type_name}' with the given config: {e}"
        ) from e
