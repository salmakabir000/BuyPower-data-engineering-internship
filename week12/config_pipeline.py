import argparse
import importlib
import yaml


def load_config(path):
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def load_component(component_type, category):
    module = importlib.import_module(
        f"mini_orchestrator.components.{category}"
    )
    return getattr(module, component_type)


def run_pipeline(config):
    print(f"Running pipeline: {config['name']}")

    # Load source dynamically
    source_type = config["source"]["type"]
    source_config = config["source"].get("config", {})

    source_class = load_component(source_type, "sources")
    source = source_class(source_config)

    data = source.extract()

    # Apply transforms dynamically
    for transform_config in config.get("transforms", []):
        transform_type = transform_config["type"]

        transform_class = load_component(
            transform_type,
            "transforms"
        )

        transform = transform_class(transform_config)
        data = transform.apply(data)

    # Load destination dynamically
    destination_type = config["destination"]["type"]
    destination_config = config["destination"].get("config", {})

    destination_class = load_component(
        destination_type,
        "destinations"
    )

    destination = destination_class(destination_config)
    destination.load(data)

    print("Pipeline completed.")


def validate_config(config):
    required = [
        "name",
        "source",
        "transforms",
        "destination",
    ]

    for field in required:
        if field not in config:
            raise ValueError(
                f"Missing required field: {field}"
            )

    if "type" not in config["source"]:
        raise ValueError("Source type is missing")

    if "type" not in config["destination"]:
        raise ValueError("Destination type is missing")

    if not isinstance(config["transforms"], list):
        raise ValueError("Transforms must be a list")

    print(
        f"Configuration is valid: {config['name']}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Config-driven data pipeline"
    )

    subparsers = parser.add_subparsers(
        dest="command"
    )

    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("config")

    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument("config")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    try:
        config = load_config(args.config)

        if args.command == "validate":
            validate_config(config)

        elif args.command == "run":
            validate_config(config)
            run_pipeline(config)

    except FileNotFoundError:
        print(
            f"Error: Config file not found: {args.config}"
        )

    except yaml.YAMLError as error:
        print(
            f"Error: Invalid YAML: {error}"
        )

    except ValueError as error:
        print(
            f"Error: Invalid configuration: {error}"
        )


if __name__ == "__main__":
    main()