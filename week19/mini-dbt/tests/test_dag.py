
import pytest

from mini_dbt.dag import (
    build_graph,
    execution_order,
    select_downstream,
    select_upstream,
)


models = [
    {"name": "stg_orders", "refs": []},
    {"name": "stg_customers", "refs": []},
    {"name": "fact_orders", "refs": ["stg_orders"]},
    {"name": "dim_customers", "refs": ["stg_customers"]},
    {
        "name": "customer_summary",
        "refs": ["fact_orders", "dim_customers"],
    },
]


def test_execution_order():
    graph = build_graph(models)
    order = execution_order(graph)

    assert order.index("stg_orders") < order.index("fact_orders")
    assert order.index("stg_customers") < order.index("dim_customers")
    assert order.index("fact_orders") < order.index("customer_summary")
    assert order.index("dim_customers") < order.index("customer_summary")


def test_circular_dependency():
    graph = {
        "model_a": {"model_b"},
        "model_b": {"model_a"},
    }

    with pytest.raises(ValueError, match="Circular dependency"):
        execution_order(graph)


def test_downstream_selection():
    graph = build_graph(models)

    selected = select_downstream(graph, "fact_orders")

    assert selected == {"fact_orders", "customer_summary"}

def test_upstream_selection():
    graph = {
        "stg_events": set(),
        "stg_actors": set(),
        "stg_repos": set(),
        "stg_event_types": set(),
        "stg_dates": set(),
        "mart_event_details": {
            "stg_events",
            "stg_actors",
            "stg_repos",
            "stg_event_types",
            "stg_dates",
        },
    }

    selected = select_upstream(graph, "mart_event_details")

    assert selected == {
        "mart_event_details",
        "stg_events",
        "stg_actors",
        "stg_repos",
        "stg_event_types",
        "stg_dates",
    }
