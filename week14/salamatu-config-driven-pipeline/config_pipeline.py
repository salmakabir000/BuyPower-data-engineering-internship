import argparse
import importlib
from pathlib import Path

import yaml


SOURCE_FIELDS = {
    "csv_file": {"path"},
    "parquet_file": {"path"},
    "rest_api": {"url"},
}

DESTINATION_FIELDS = {
    "sqlite": {"database", "table"},
    "parquet_file": {"path"},
}

TRANSFORM_FIELDS = {
    "select_columns": {"columns"},
    "rename_column": {"mapping"},
    "add_column": {"column", "value"},
    "filter_rows": {"column", "operator"},
    "cast_column": {"column", "dtype"},
}


def load_config(config_path):
    try:
        with open(config_path, "r", encoding="utf-8") as file:
            config = yaml.safe_load(file)
    except FileNotFoundError as exc:
        raise ValueError(f"Configuration file not found: {config_path}") from exc
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid YAML syntax in {config_path}: {exc}") from exc

    if not isinstance(config, dict):
        raise ValueError("Configuration must be a YAML mapping.")

    return config


def check_fields(section, config, allowed_fields):
    if not isinstance(config, dict):
        raise ValueError(f"'{section}' must be a YAML mapping.")

    component_type = config.get("type")
    if component_type not in allowed_fields:
        raise ValueError(
            f"Unsupported {section} type: {component_type!r}. "
            f"Supported types: {', '.join(allowed_fields)}"
        )

    missing = allowed_fields[component_type] - config.keys()
    if missing:
        raise ValueError(
            f"Missing required fields in '{section}': {', '.join(sorted(missing))}"
        )


def validate_config(config):
    if "source" not in config:
        raise ValueError("Missing required section: source")

    if "destination" not in config:
        raise ValueError("Missing required section: destination")

    check_fields("source", config["source"], SOURCE_FIELDS)
    check_fields("destination", config["destination"], DESTINATION_FIELDS)

    transforms = config.get("transforms", [])
    if not isinstance(transforms, list):
        raise ValueError("'transforms' must be a YAML list.")

    for index, transform in enumerate(transforms):
        section = f"transforms[{index}]"
        check_fields(section, transform, TRANSFORM_FIELDS)

        if transform["type"] == "filter_rows":
            if transform.get("operator") not in {
                "equals", "not_equals", "greater_than",
                "less_than", "is_not_null",
            }:
                raise ValueError(
                    f"Unsupported filter operator: {transform.get('operator')!r}"
                )

        if transform["type"] == "cast_column":
            if transform.get("dtype") not in {
                "string", "integer", "float", "boolean",
            }:
                raise ValueError(
                    f"Unsupported data type: {transform.get('dtype')!r}"
                )

    return True


def load_component(category, component_type):
    module_path = f"components.{category}.{component_type}"
    return importlib.import_module(module_path)


def run_pipeline(config_path):
    config = load_config(config_path)
    validate_config(config)

    source = load_component("sources", config["source"]["type"])
    df = source.read(config["source"])
    print(f"Read {len(df)} rows from source.")

    for transform_config in config.get("transforms", []):
        transform = load_component("transforms", transform_config["type"])
        df = transform.apply(df, transform_config)
        print(
            f"Applied {transform_config['type']}: "
            f"{len(df)} rows, {len(df.columns)} columns"
        )

    destination = load_component(
        "destinations", config["destination"]["type"]
    )
    destination.write(df, config["destination"])


def main():
    parser = argparse.ArgumentParser(description="Config-driven data pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run a pipeline")
    run_parser.add_argument("config", help="Path to YAML configuration")

    validate_parser = subparsers.add_parser(
        "validate", help="Validate a pipeline configuration"
    )
    validate_parser.add_argument("config", help="Path to YAML configuration")

    args = parser.parse_args()

    try:
        config = load_config(args.config)

        if args.command == "validate":
            validate_config(config)
            print(f"Configuration is valid: {args.config}")
        elif args.command == "run":
            validate_config(config)
            run_pipeline(args.config)
    except (ValueError, KeyError, ImportError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
