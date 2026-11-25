import argparse
import psycopg2

from mini_dbt.config import load_config
from mini_dbt.parser import discover_models
from mini_dbt.dag import (
    build_graph,
    execution_order,
    select_downstream,
    select_upstream,
)
from mini_dbt.runner import run_models
from mini_dbt.docs import generate_docs

def load_project(require_database=False):
    config = load_config(
        "example_project/project.yml",
        "profiles.yml",
        require_database=require_database
    )

    default_materialization = config["project"]["models"].get(
        "materialized", "table"
    )

    models = discover_models(
        "example_project/models",
        default_materialization
    )

    graph = build_graph(models)

    return config, models, graph


def compile_project():
    config, models, graph = load_project()

    order = execution_order(graph)

    print(f"Project: {config['project']['name']}")
    print(f"Models discovered: {len(models)}")
    print("\nExecution order:")

    for position, model_name in enumerate(order, start=1):
        print(f"{position}. {model_name}")

    print("\nCompilation successful!")


def run_project(selection=None):
    config, models, graph = load_project(require_database=True)

    if selection:
        if selection.endswith("+"):
            model_name = selection[:-1]
            selected = select_downstream(graph, model_name)
        else:
            if selection not in graph:
                raise ValueError(f"Unknown model: {selection}")
            selected = {selection}

        # Include all upstream dependencies.
        required = set()

        for model_name in selected:
            required.update(select_upstream(graph, model_name))

        selected = required

        order = [
            name for name in execution_order(graph)
            if name in selected
        ]
    else:
        order = execution_order(graph)

    database = config["database"]

    connection = psycopg2.connect(
        host=database["host"],
        port=database["port"],
        user=database["user"],
        password=database["password"],
        dbname=database["database"]
    )

    try:
        run_models(connection, models, order, config)
    finally:
        connection.close()

def generate_project_docs():
    config, models, graph = load_project()

    generate_docs(
        config["project"]["name"],
        models,
        graph
    )


def main():
    parser = argparse.ArgumentParser(
        description="Mini dbt - SQL transformation framework"
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True
    )

    subparsers.add_parser("compile")
    subparsers.add_parser("docs")

    run_parser = subparsers.add_parser("run")
    run_parser.add_argument(
        "--select",
        help="Select a model or a model and its downstream models"
    )

    args = parser.parse_args()

    if args.command == "compile":
        compile_project()

    elif args.command == "run":
        run_project(args.select)

    elif args.command == "docs":
        generate_project_docs()

if __name__ == "__main__":
    main()
