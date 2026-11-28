from pathlib import Path
from psycopg2.extras import execute_values
import gzip
import json
import os
import psycopg2


def get_connection():
    return psycopg2.connect(
        dbname="github_warehouse",
        user="warehouse_loader",
        password=os.environ["DB_PASSWORD"],
        host="localhost",
        port="5432"
    )


def load_event_types(connection, event_types):
    with connection.cursor() as cursor:
        for event_type in event_types:
            cursor.execute(
                """
                INSERT INTO dim_event_type (event_type_name)
                VALUES (%s)
                ON CONFLICT (event_type_name) DO NOTHING;
                """,
                (event_type,)
            )

    connection.commit()


def extract_event_types(file_path):
    event_types = set()

    with gzip.open(file_path, "rt") as file:
        for line in file:
            event = json.loads(line)
            event_types.add(event["type"])

    return event_types

def extract_actors(file_path):
    actors = {}

    with gzip.open(file_path, "rt") as file:
        for line in file:
            event = json.loads(line)
            actor = event["actor"]

            actor_id = actor["id"]

            actors[actor_id] = (
                actor_id,
                actor.get("login"),
                actor.get("display_login"),
                actor.get("url")
            )

    return list(actors.values())

def load_actors(connection, actors):
    with connection.cursor() as cursor:
        execute_values(
            cursor,
            """
            INSERT INTO dim_actor (actor_id, login, display_name, url)
            VALUES %s
            ON CONFLICT (actor_id) DO NOTHING;
            """,
            actors
        )

    connection.commit()

def extract_repos(file_path):
    repos = {}

    with gzip.open(file_path, "rt") as file:
        for line in file:
            event = json.loads(line)
            repo = event["repo"]

            repo_id = repo["id"]

            repos[repo_id] = (
                repo_id,
                repo.get("name"),
                repo.get("url")
            )

    return list(repos.values())

def load_repos(connection, repos):
    with connection.cursor() as cursor:
        execute_values(
            cursor,
            """
            INSERT INTO dim_repo (repo_id, repo_name, repo_url)
            VALUES %s
            ON CONFLICT (repo_id) DO NOTHING;
            """,
            repos
        )

    connection.commit()

def get_dimension_lookups(connection):
    with connection.cursor() as cursor:

        cursor.execute(
            "SELECT actor_id, actor_sk FROM dim_actor"
        )
        actor_lookup = dict(cursor.fetchall())

        cursor.execute(
            "SELECT repo_id, repo_sk FROM dim_repo"
        )
        repo_lookup = dict(cursor.fetchall())

        cursor.execute(
            "SELECT event_type_name, event_type_sk FROM dim_event_type"
        )
        event_type_lookup = dict(cursor.fetchall())

    return actor_lookup, repo_lookup, event_type_lookup

def extract_fact_records(file_path, actor_lookup, repo_lookup, event_type_lookup):
    records = []

    with gzip.open(file_path, "rt") as file:
        for line in file:
            event = json.loads(line)

            event_id = int(event["id"])
            event_type = event["type"]
            actor_id = event["actor"]["id"]
            repo_id = event["repo"]["id"]
            created_at = event["created_at"]

            actor_sk = actor_lookup.get(actor_id, -1)
            repo_sk = repo_lookup.get(repo_id, -1)
            event_type_sk = event_type_lookup.get(event_type, -1)

            date_sk = int(created_at[:10].replace("-", ""))

            records.append((
                event_id,
                event_type_sk,
                actor_sk,
                repo_sk,
                date_sk,
                created_at
            ))

    return records

def load_fact_events(connection, records):
    with connection.cursor() as cursor:
        execute_values(
            cursor,
            """
            INSERT INTO fact_events (
                event_id,
                event_type_sk,
                actor_sk,
                repo_sk,
                date_sk,
                created_at
            )
            VALUES %s
            ON CONFLICT (event_id) DO NOTHING;
            """,
            records
        )

    connection.commit()


if __name__ == "__main__":
    raw_dir = Path("week7/data/raw")
    file_paths = sorted(raw_dir.glob("*.json.gz"))

    connection = get_connection()

    try:
        for file_path in file_paths:
            print(f"\nProcessing: {file_path.name}")

            event_types = extract_event_types(file_path)
            load_event_types(connection, event_types)

            actors = extract_actors(file_path)
            load_actors(connection, actors)

            repos = extract_repos(file_path)
            load_repos(connection, repos)

            actor_lookup, repo_lookup, event_type_lookup = (
                get_dimension_lookups(connection)
            )

            records = extract_fact_records(
                file_path,
                actor_lookup,
                repo_lookup,
                event_type_lookup
            )

            print(f"Fact records extracted: {len(records)}")

            load_fact_events(connection, records)

        print("\nAll files processed successfully.")

    finally:
        connection.close()
