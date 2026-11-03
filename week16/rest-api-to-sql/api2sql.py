import argparse
import time
import os

import psycopg
import requests
import yaml
from psycopg.types.json import Jsonb

last_request_time = 0


def load_config(path):
    with open(path, "r") as file:
        return yaml.safe_load(file)

def get_last_synced_at(config):
    conn_string = os.environ.get(config["destination"]["conn_env"])

    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT last_synced_at
                FROM pipeline_state
                WHERE pipeline_name = %s
                """,
                (config["name"],),
            )
            result = cursor.fetchone()

    return result[0] if result else None

last_request_time = 0


def github_get(url, params=None, config=None):
    global last_request_time

    token = os.environ.get("GITHUB_TOKEN")

    if not token:
        raise ValueError("Missing GITHUB_TOKEN environment variable")

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }

    requests_per_minute = config["rate_limit"]["requests_per_minute"]
    min_interval = 60 / requests_per_minute

    for attempt in range(5):

        elapsed = time.monotonic() - last_request_time
        wait = min_interval - elapsed

        if wait > 0:
            time.sleep(wait)

        last_request_time = time.monotonic()

        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=30,
        )

        if response.status_code == 429 or response.status_code in (500, 502, 503, 504):
            if attempt == 4:
                response.raise_for_status()

            wait = int(response.headers.get("Retry-After", 2 ** attempt))
            print(f"Request failed temporarily. Retrying in {wait} seconds...")
            time.sleep(wait)
            continue

        if response.status_code == 403 and response.headers.get("X-RateLimit-Remaining") == "0":
            raise RuntimeError(
                "GitHub API rate limit exhausted. Try again after the reset time."
            )

        response.raise_for_status()
        return response

    raise RuntimeError("GitHub request failed after 5 attempts")


def fetch_records(config, last_synced_at=None):
    url = config["base_url"] + config["endpoint"]

    pagination = config["pagination"]
    pagination_type = pagination["type"]
    per_page = pagination["per_page"]

    params = {"per_page": per_page}

    if last_synced_at:
        params[config["incremental"]["cursor_param"]] = (
            last_synced_at.isoformat()
        )

    records = []
    page_number = pagination.get("start_page", 1)
    cursor = None

    while url:
        if pagination_type == "link_header":
            pass

        elif pagination_type == "cursor":
            if cursor:
                params[pagination["cursor_param"]] = cursor

        elif pagination_type == "page":
            params[pagination["page_param"]] = page_number

        else:
            raise ValueError(
                f"Unsupported pagination type: {pagination_type}"
            )

        response = github_get(url, params=params, config=config)
        data = response.json()

        if isinstance(data, list):
            page_records = data
        else:
            page_records = data[pagination.get("records_field", "data")]

        records.extend(page_records)

        print(
            f"Retrieved {len(page_records)} records; "
            f"total so far: {len(records)}"
        )

        if pagination_type == "link_header":
            url = response.links.get("next", {}).get("url")
            params = None

        elif pagination_type == "cursor":
            cursor = data.get(pagination["next_cursor_field"])
            if not cursor:
                break

        elif pagination_type == "page":
            if len(page_records) < per_page:
                break
            page_number += 1

    return records



def load_records(records, config, full_refresh=False):
    conn_string = os.environ.get(config["destination"]["conn_env"])

    if not conn_string:
        raise ValueError(
            f"Missing environment variable: "
            f"{config['destination']['conn_env']}"
        )

    table = config["destination"]["table"]

    from psycopg import sql

    insert_query = sql.SQL("""
        INSERT INTO {} (
            id,
            number,
            title,
            state,
            author_login,
            created_at,
            updated_at,
            is_pull_request,
            raw_data
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (id) DO UPDATE SET
            number = EXCLUDED.number,
            title = EXCLUDED.title,
            state = EXCLUDED.state,
            author_login = EXCLUDED.author_login,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at,
            is_pull_request = EXCLUDED.is_pull_request,
            raw_data = EXCLUDED.raw_data
    """).format(sql.Identifier(table))

    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cursor:

            if full_refresh:
                cursor.execute(
                    sql.SQL("TRUNCATE TABLE {}").format(
                        sql.Identifier(table)
                    )
                )
                print(f"Cleared existing data from {table}.")

                cursor.execute(
                    """
                    DELETE FROM pipeline_state
                    WHERE pipeline_name = %s
                    """,
                    (config["name"],),
                )

            for record in records:
                user = record.get("user") or {}

                cursor.execute(
                    insert_query,
                    (
                        record["id"],
                        record["number"],
                        record.get("title"),
                        record.get("state"),
                        user.get("login"),
                        record.get("created_at"),
                        record.get("updated_at"),
                        "pull_request" in record,
                        Jsonb(record),
                    ),
                )

            if records:
                cursor.execute(
                    sql.SQL(
                        "SELECT MAX(updated_at) FROM {}"
                    ).format(sql.Identifier(table))
                )

                latest_timestamp = cursor.fetchone()[0]

                cursor.execute(
                    """
                    INSERT INTO pipeline_state (
                        pipeline_name,
                        last_synced_at
                    )
                    VALUES (%s, %s)
                    ON CONFLICT (pipeline_name)
                    DO UPDATE SET
                        last_synced_at = EXCLUDED.last_synced_at
                    """,
                    (config["name"], latest_timestamp),
                )

    print(f"Loaded or updated {len(records)} records in {table}.")

def show_status(config):
    conn_string = os.environ.get(config["destination"]["conn_env"])
    table = config["destination"]["table"]

    from psycopg import sql

    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("SELECT COUNT(*) FROM {}").format(
                    sql.Identifier(table)
                )
            )
            total_records = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT last_synced_at
                FROM pipeline_state
                WHERE pipeline_name = %s
                """,
                (config["name"],),
            )
            result = cursor.fetchone()

    print(f"Pipeline: {config['name']}")
    print(f"Destination: {table}")
    print(f"Total records: {total_records}")
    print(f"Last synced: {result[0] if result else 'Never'}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="REST API to SQL pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run the pipeline")
    run_parser.add_argument("config", help="Path to YAML configuration")
    run_parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Replace existing destination data",
    )

    status_parser = subparsers.add_parser("status", help="Show pipeline status")
    status_parser.add_argument(
        "config",
        nargs="?",
        default="configs/github_issues.yml",
    )

    args = parser.parse_args()

    config = load_config(args.config)

    if args.command == "run":
        last_synced_at = None

        if not args.full_refresh:
            last_synced_at = get_last_synced_at(config)

        if last_synced_at:
            print(f"Starting incremental sync from: {last_synced_at}")
        elif args.full_refresh:
            print("Starting full refresh.")
        else:
            print("No previous checkpoint found. Starting initial sync.")

        issues = fetch_records(config, last_synced_at)
        load_records(issues, config, full_refresh=args.full_refresh)

    elif args.command == "status":
        show_status(config)
