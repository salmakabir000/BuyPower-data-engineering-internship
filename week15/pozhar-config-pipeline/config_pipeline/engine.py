"""
engine.py - reads a pipeline's YAML, validates it, and (for run_pipeline)
executes it. This file has NO knowledge of CoinGecko, weather, or any
specific data source - it only ever deals with the generic shape every
pipeline YAML must have: name / source / transforms / destination.
"""

from pathlib import Path

import yaml

from .registry import build_component, load_component_class, ConfigError


def load_yaml(path):
    path = Path(path)
    if not path.exists():
        raise ConfigError(f"Pipeline file not found: {path}")
    try:
        with open(path) as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigError(f"'{path}' is not valid YAML: {e}") from e

    if not isinstance(config, dict):
        raise ConfigError(f"'{path}' must contain a YAML mapping (key: value pairs) at the top level")
    return config


def validate_config(config, source_path="<config>"):
    """Checks a parsed config's structure and makes sure every referenced
    component type actually exists - without instantiating or running
    anything. Raises ConfigError with a specific message on the first
    problem found."""

    def require(d, key, kind_desc):
        if key not in d:
            raise ConfigError(f"{source_path}: missing required field '{key}' in {kind_desc}")
        return d[key]

    require(config, "name", "pipeline")
    source_cfg = require(config, "source", "pipeline")
    destination_cfg = require(config, "destination", "pipeline")
    transforms_cfg = config.get("transforms", [])

    if not isinstance(transforms_cfg, list):
        raise ConfigError(f"{source_path}: 'transforms' must be a list")

    source_type = require(source_cfg, "type", "source")
    load_component_class("source", source_type)  # raises ConfigError if unknown

    for i, t_cfg in enumerate(transforms_cfg):
        t_type = require(t_cfg, "type", f"transforms[{i}]")
        load_component_class("transform", t_type)

    destination_type = require(destination_cfg, "type", "destination")
    load_component_class("destination", destination_type)

    return True  # config is structurally valid and every type resolves


def build_pipeline(config):
    """Instantiates the source, transform chain, and destination described
    by a validated config. Returns (source, transforms, destination)."""
    source = build_component("source", config["source"]["type"], config["source"].get("config"))
    # unlike source/destination, a transform's fields sit directly next to
    # `type:` rather than under a nested `config:` key (matches the spec's
    # own example YAML) - so its config is just everything except "type"
    transforms = [
        build_component("transform", t["type"], {k: v for k, v in t.items() if k != "type"})
        for t in config.get("transforms", [])
    ]
    destination = build_component(
        "destination", config["destination"]["type"], config["destination"].get("config")
    )
    return source, transforms, destination


def run_pipeline(path, verbose=True):
    config = load_yaml(path)
    validate_config(config, source_path=str(path))

    name = config["name"]
    if verbose:
        print(f"Running pipeline '{name}' ({path})")

    source, transforms, destination = build_pipeline(config)

    if verbose:
        print(f"  [SOURCE] {config['source']['type']} - fetching...")
    df = source.fetch()
    if verbose:
        print(f"  [SOURCE] fetched {len(df)} rows, {len(df.columns)} columns")

    for t_cfg, transform in zip(config.get("transforms", []), transforms):
        df = transform.apply(df)
        if verbose:
            print(f"  [TRANSFORM] {t_cfg['type']} -> {len(df)} rows, {len(df.columns)} columns")

    if verbose:
        print(f"  [DESTINATION] {config['destination']['type']} - writing...")
    result = destination.write(df)
    if verbose:
        print(f"  [DESTINATION] done: {result}")

    return {"name": name, "rows": len(df), "destination_result": result}
