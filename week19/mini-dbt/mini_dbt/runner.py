import re
from psycopg2 import sql


REF_PATTERN = re.compile(
    r"\{\{\s*ref\(['\"]([\w_]+)['\"]\)\s*\}\}"
)

SOURCE_PATTERN = re.compile(
    r"\{\{\s*source\(['\"]([\w_]+)['\"],\s*['\"]([\w_]+)['\"]\)\s*\}\}"
)


def render_sql(model_sql, target_schema, models):
    def replace_ref(match):
        model_name = match.group(1)

        if model_name not in models:
            raise ValueError(f"Unknown model reference: {model_name}")

        return f'"{target_schema}"."{model_name}"'

    def replace_source(match):
        source_name = match.group(1)
        table_name = match.group(2)

        sources = models["sources"]

        if source_name not in sources:
            raise ValueError(f"Unknown source: {source_name}")

        source_schema = sources[source_name]["schema"]

        return f'"{source_schema}"."{table_name}"'

    rendered = REF_PATTERN.sub(replace_ref, model_sql)
    rendered = SOURCE_PATTERN.sub(replace_source, rendered)

    return rendered

def materialize_model(
    cursor, model, target_schema, rendered_sql, default_materialization="table"
):
    model_name = model["name"]
    materialization = model.get("materialized", default_materialization)

    schema_identifier = sql.Identifier(target_schema)
    model_identifier = sql.Identifier(model_name)

    if materialization == "table":
        statement = sql.SQL(
            "CREATE TABLE {}.{} AS "
        ).format(schema_identifier, model_identifier)

    elif materialization == "view":
        statement = sql.SQL(
            "CREATE VIEW {}.{} AS "
        ).format(schema_identifier, model_identifier)

    else:
        raise ValueError(
            f"Unsupported materialization: {materialization}"
        )

    cursor.execute(statement + sql.SQL(rendered_sql))

def drop_existing_model(cursor, target_schema, model_name):
    cursor.execute(
        """
        SELECT relkind
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s
          AND c.relname = %s
          AND c.relkind IN ('r', 'p', 'v', 'm')
        """,
        (target_schema, model_name)
    )

    result = cursor.fetchone()

    if result is None:
        return

    object_type = result[0]

    if object_type in ("r", "p"):
        drop_statement = "DROP TABLE {}.{}"
    elif object_type == "v":
        drop_statement = "DROP VIEW {}.{}"
    else:
        raise ValueError(
            f"Unsupported existing object type for {model_name}: {object_type}"
        )

    cursor.execute(
        sql.SQL(drop_statement).format(
            sql.Identifier(target_schema),
            sql.Identifier(model_name)
        )
    )


def run_models(connection, parsed_models, execution_order, config):
    project = config["project"]

    target_schema = project["models"]["schema"]
    sources = project.get("sources", {})

    model_lookup = {
        model["name"]: model
        for model in parsed_models
    }

    # Add source configuration for the SQL resolver.
    model_lookup["sources"] = sources

    try:
        with connection.cursor() as cursor:
            cursor.execute(
               """
               SELECT EXISTS (
                   SELECT 1
                   FROM information_schema.schemata
                   WHERE schema_name = %s
               )
               """,
               (target_schema,)
            )

            if not cursor.fetchone()[0]:
                raise ValueError(
                    f"Target schema does not exist: {target_schema}"
               )

            # Remove old outputs in reverse dependency order.
            for model_name in reversed(execution_order):
                drop_existing_model(cursor, target_schema, model_name)


            # Build models in dependency order.
            for model_name in execution_order:
                model = model_lookup[model_name]

                rendered_sql = render_sql(
                    model["sql"],
                    target_schema,
                    model_lookup
                )

                materialize_model(
                    cursor,
                    model,
                    target_schema,
                    rendered_sql,
                    project["models"].get("materialized", "table")
                )

                print(f"Built {target_schema}.{model_name}")

        connection.commit()

    except Exception:
        connection.rollback()
        raise
