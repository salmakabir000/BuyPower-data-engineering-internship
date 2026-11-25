import pytest

from mini_dbt.runner import (
    materialize_model,
    render_sql,
    drop_existing_model,
)


def test_render_sql():
    model_sql = """
    SELECT *
    FROM {{ source('warehouse', 'fact_events') }}
    JOIN {{ ref('stg_actors') }}
    ON actor_sk = actor_sk
    """

    models = {
        "stg_actors": {},
        "sources": {
            "warehouse": {
                "schema": "public"
            }
        }
    }

    result = render_sql(
        model_sql,
        target_schema="analytics",
        models=models
    )

    assert '"public"."fact_events"' in result
    assert '"analytics"."stg_actors"' in result


def test_unknown_model_reference():
    model_sql = "SELECT * FROM {{ ref('missing_model') }}"

    with pytest.raises(ValueError, match="Unknown model"):
        render_sql(
            model_sql,
            target_schema="analytics",
            models={"sources": {}}
        )


def test_unknown_source():
    model_sql = "SELECT * FROM {{ source('missing_source', 'events') }}"

    with pytest.raises(ValueError, match="Unknown source"):
        render_sql(
            model_sql,
            target_schema="analytics",
            models={"sources": {}}
        )


class FakeCursor:
    def __init__(self, result=None):
        self.executed = []
        self.result = result

    def execute(self, statement, params=None):
        self.executed.append((statement, params))

    def fetchone(self):
        return self.result


def test_materialize_table():
    cursor = FakeCursor()

    materialize_model(
        cursor,
        {"name": "stg_events"},
        "analytics",
        "SELECT * FROM events"
    )

    assert len(cursor.executed) == 1


def test_materialize_view():
    cursor = FakeCursor()

    materialize_model(
        cursor,
        {"name": "stg_events", "materialized": "view"},
        "analytics",
        "SELECT * FROM events"
    )

    assert len(cursor.executed) == 1


def test_unsupported_materialization():
    cursor = FakeCursor()

    with pytest.raises(ValueError, match="Unsupported materialization"):
        materialize_model(
            cursor,
            {"name": "stg_events", "materialized": "incremental"},
            "analytics",
            "SELECT * FROM events"
        )

    assert len(cursor.executed) == 0

def test_drop_skips_when_model_does_not_exist():
    cursor = FakeCursor(result=None)

    drop_existing_model(cursor, "analytics", "stg_events")

    assert len(cursor.executed) == 1


def test_drop_existing_table():
    cursor = FakeCursor(result=("r",))

    drop_existing_model(cursor, "analytics", "stg_events")

    assert len(cursor.executed) == 2


def test_drop_existing_view():
    cursor = FakeCursor(result=("v",))

    drop_existing_model(cursor, "analytics", "stg_events")

    assert len(cursor.executed) == 2


def test_unsupported_existing_object():
    cursor = FakeCursor(result=("i",))

    with pytest.raises(ValueError, match="Unsupported existing object type"):
        drop_existing_model(cursor, "analytics", "stg_events")

    assert len(cursor.executed) == 1
