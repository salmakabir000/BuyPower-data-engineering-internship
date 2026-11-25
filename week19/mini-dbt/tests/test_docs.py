from mini_dbt.docs import generate_docs


def test_generate_docs(tmp_path):
    models = [
        {
            "name": "stg_events",
            "sql": "SELECT * FROM public.fact_events",
            "refs": [],
            "sources": [],
            "materialized": "table",
            "columns": {
                "event_id": "Original event identifier"
            }
        },
        {
            "name": "mart_events",
            "sql": "SELECT * FROM {{ ref('stg_events') }}",
            "refs": ["stg_events"],
            "sources": [],
            "materialized": "table",
            "columns": {}
        }
    ]

    graph = {
        "stg_events": set(),
        "mart_events": {"stg_events"}
    }

    output_file = tmp_path / "docs" / "index.html"

    generate_docs(
        "test_project",
        models,
        graph,
        output_file
    )

    assert output_file.exists()

    content = output_file.read_text(encoding="utf-8")

    assert "test_project" in content
    assert "stg_events" in content
    assert "mart_events" in content
    assert "Original event identifier" in content
    assert "Dependencies:" in content
    assert "SELECT * FROM" in content
