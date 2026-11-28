from mini_dbt.parser import extract_refs, extract_sources
from mini_dbt.parser import discover_models
import pytest

def test_extract_refs():
    sql = """
    SELECT *
    FROM {{ ref('stg_orders') }}
    JOIN {{ ref("dim_customers") }}
    ON customer_id = id
    """

    assert extract_refs(sql) == [
        "stg_orders",
        "dim_customers"
    ]


def test_extract_sources():
    sql = "SELECT * FROM {{ source('github', 'events') }}"

    assert extract_sources(sql) == [
        ("github", "events")
    ]


def test_discover_models(tmp_path):
    staging = tmp_path / "staging"
    marts = tmp_path / "marts"

    staging.mkdir()
    marts.mkdir()

    (staging / "stg_events.sql").write_text(
        "SELECT * FROM {{ source('warehouse', 'fact_events') }}"
    )

    (marts / "mart_events.sql").write_text(
        "SELECT * FROM {{ ref('stg_events') }}"
    )

    models = discover_models(tmp_path)

    assert len(models) == 2
    assert models[0]["name"] == "mart_events"
    assert models[1]["name"] == "stg_events"
    assert models[0]["refs"] == ["stg_events"]
    assert models[0]["materialized"] == "table"


def test_duplicate_model_names(tmp_path):
    (tmp_path / "staging").mkdir()
    (tmp_path / "marts").mkdir()

    (tmp_path / "staging" / "stg_events.sql").write_text("SELECT 1")
    (tmp_path / "marts" / "stg_events.sql").write_text("SELECT 2")

    with pytest.raises(ValueError, match="Duplicate model names"):
        discover_models(tmp_path)
