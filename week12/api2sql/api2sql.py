import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

import psycopg2
import requests
import yaml


RETRY_STATUS_CODES = {429, 500, 502, 503, 504}


def load_config(config_path):
    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def get_env(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable is not set: {name}")
    return value


class RestClient:
    def __init__(self, config):
        self.base_url = config["source"]["base_url"].rstrip("/")
        self.endpoint = config["source"]["endpoint"]

        auth = config["source"].get("auth", {})
        token_env = auth.get("token_env")

        self.headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "api2sql",
        }

        if token_env:
            token = os.getenv(token_env)
            if token:
                self.headers["Authorization"] = f"Bearer {token}"

        rate_limit = config["source"].get("rate_limit", {})
        requests_per_minute = rate_limit.get("requests_per_minute", 60)

        self.request_delay = (
            60 / requests_per_minute if requests_per_minute else 0
        )

        retry_config = config["source"].get("retry", {})
        self.max_retries = retry_config.get("max_retries", 3)
        self.backoff_seconds = retry_config.get("backoff_seconds", 2)

    def get(self, url, params=None):
        for attempt in range(self.max_retries + 1):
            try:
                response = requests.get(
                    url,
                    headers=self.headers,
                    params=params,
                    timeout=30,
                )

                if response.status_code == 200:
                    time.sleep(self.request_delay)
                    return response

                if response.status_code in RETRY_STATUS_CODES:
                    if attempt == self.max_retries:
                        response.raise_for_status()

                    retry_after = response.headers.get("Retry-After")

                    if retry_after:
                        try:
                            wait_time = float(retry_after)
                        except ValueError:
                            wait_time = self.backoff_seconds * (2**attempt)
                    else:
                        wait_time = self.backoff_seconds * (2**attempt)

                    print(
                        f"Request returned {response.status_code}. "
                        f"Retrying in {wait_time:.1f}s..."
                    )

                    time.sleep(wait_time)
                    continue

                response.raise_for_status()

            except requests.RequestException as error:
                if attempt == self.max_retries:
                    raise

                wait_time = self.backoff_seconds * (2**attempt)

                print(
                    f"Request failed: {error}. "
                    f"Retrying in {wait_time:.1f}s..."
                )

                time.sleep(wait_time)

        raise RuntimeError("Request failed after all retries")


def get_connection(config):
    destination = config["destination"]

    connection_env = destination.get("conn_env", "PG_CONN")
    connection_string = get_env(connection_env)

    return psycopg2.connect(connection_string)


def ensure_tables(connection, table_name):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS api2sql_state (
                source_name TEXT PRIMARY KEY,
                cursor_value TEXT
            )
            """
        )

        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                id BIGINT PRIMARY KEY,
                updated_at TIMESTAMPTZ,
                data JSONB NOT NULL
            )
            """
        )

    connection.commit()


def get_saved_cursor(connection, source_name):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT cursor_value
            FROM api2sql_state
            WHERE source_name = %s
            """,
            (source_name,),
        )

        row = cursor.fetchone()

    return row[0] if row else None


def save_cursor(connection, source_name, cursor_value):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO api2sql_state (source_name, cursor_value)
            VALUES (%s, %s)
            ON CONFLICT (source_name)
            DO UPDATE SET cursor_value = EXCLUDED.cursor_value
            """,
            (source_name, cursor_value),
        )

    connection.commit()


def upsert_records(connection, table_name, records):
    if not records:
        return

    with connection.cursor() as cursor:
        for record in records:
            record_id = record.get("id")

            if record_id is None:
                continue

            updated_at = record.get("updated_at")

            cursor.execute(
                f"""
                INSERT INTO "{table_name}" (id, updated_at, data)
                VALUES (%s, %s, %s)
                ON CONFLICT (id)
                DO UPDATE SET
                    updated_at = EXCLUDED.updated_at,
                    data = EXCLUDED.data
                """,
                (
                    record_id,
                    updated_at,
                    json.dumps(record),
                ),
            )

    connection.commit()


def get_next_link(response):
    link_header = response.headers.get("Link")

    if not link_header:
        return None

    links = link_header.split(",")

    for link in links:
        parts = link.split(";")

        if len(parts) < 2:
            continue

        url_part = parts[0].strip()

        if 'rel="next"' not in parts[1]:
            continue

        if url_part.startswith("<") and url_part.endswith(">"):
            return url_part[1:-1]

    return None


def fetch_link_header(client, config, saved_cursor):
    source = config["source"]
    params = dict(source.get("params", {}))

    incremental = source.get("incremental", {})

    if saved_cursor and incremental.get("cursor_param"):
        params[incremental["cursor_param"]] = saved_cursor

    url = client.base_url + client.endpoint
    records = []

    while url:
        response = client.get(url, params=params)
        data = response.json()

        if isinstance(data, list):
            page_records = data
        elif isinstance(data, dict):
            page_records = data.get("items", [])
        else:
            page_records = []

        records.extend(page_records)

        url = get_next_link(response)

        params = None

    return records


def fetch_page_based(client, config, saved_cursor):
    source = config["source"]
    base_params = dict(source.get("params", {}))

    incremental = source.get("incremental", {})

    if saved_cursor and incremental.get("cursor_param"):
        base_params[incremental["cursor_param"]] = saved_cursor

    page = 1
    records = []

    while True:
        params = dict(base_params)
        params["page"] = page

        response = client.get(
            client.base_url + client.endpoint,
            params=params,
        )

        data = response.json()

        if isinstance(data, list):
            page_records = data
        elif isinstance(data, dict):
            page_records = data.get("items", [])
        else:
            page_records = []

        if not page_records:
            break

        records.extend(page_records)
        page += 1

    return records


def fetch_cursor_based(client, config, saved_cursor):
    source = config["source"]
    params = dict(source.get("params", {}))

    pagination = source.get("pagination", {})
    cursor_param = pagination.get("cursor_param", "cursor")
    cursor_field = pagination.get("cursor_field", "next_cursor")

    if saved_cursor:
        params[cursor_param] = saved_cursor

    records = []

    while True:
        response = client.get(
            client.base_url + client.endpoint,
            params=params,
        )

        data = response.json()

        if isinstance(data, dict):
            page_records = data.get("items", data.get("data", []))
            next_cursor = data.get(cursor_field)
        else:
            page_records = data
            next_cursor = None

        if page_records:
            records.extend(page_records)

        if not next_cursor:
            break

        params[cursor_param] = next_cursor

    return records


def fetch_records(client, config, saved_cursor):
    pagination = config["source"].get("pagination", {})
    pagination_type = pagination.get("type", "link_header")

    if pagination_type == "link_header":
        return fetch_link_header(client, config, saved_cursor)

    if pagination_type == "page":
        return fetch_page_based(client, config, saved_cursor)

    if pagination_type == "cursor":
        return fetch_cursor_based(client, config, saved_cursor)

    raise ValueError(
        f"Unsupported pagination type: {pagination_type}"
    )


def get_latest_cursor(records, cursor_field):
    values = []

    for record in records:
        value = record.get(cursor_field)

        if value:
            values.append(value)

    if not values:
        return None

    return max(values)


def run_pipeline(config_path):
    config = load_config(config_path)

    source_name = config.get("name", "api2sql")
    destination = config["destination"]
    table_name = destination["table"]

    print(f"Starting pipeline: {source_name}")

    connection = get_connection(config)

    try:
        ensure_tables(connection, table_name)

        saved_cursor = get_saved_cursor(
            connection,
            source_name,
        )

        if saved_cursor:
            print(f"Using saved cursor: {saved_cursor}")
        else:
            print("No saved cursor found. Starting initial load.")

        client = RestClient(config)

        start_time = time.time()

        records = fetch_records(
            client,
            config,
            saved_cursor,
        )

        print(f"Records fetched: {len(records)}")

        upsert_records(
            connection,
            table_name,
            records,
        )

        incremental = config["source"].get("incremental", {})
        cursor_field = incremental.get("cursor_field")

        if cursor_field:
            latest_cursor = get_latest_cursor(
                records,
                cursor_field,
            )

            if latest_cursor:
                save_cursor(
                    connection,
                    source_name,
                    latest_cursor,
                )

                print(f"Saved cursor: {latest_cursor}")

        elapsed = time.time() - start_time

        print(
            f"Pipeline completed successfully in {elapsed:.2f} seconds."
        )

    finally:
        connection.close()


def show_status(config_path):
    config = load_config(config_path)

    source_name = config.get("name", "api2sql")
    table_name = config["destination"]["table"]

    connection = get_connection(config)

    try:
        ensure_tables(connection, table_name)

        saved_cursor = get_saved_cursor(
            connection,
            source_name,
        )

        with connection.cursor() as cursor:
            cursor.execute(
                f'SELECT COUNT(*) FROM "{table_name}"'
            )

            count = cursor.fetchone()[0]

        print(f"Source: {source_name}")
        print(f"Table: {table_name}")
        print(f"Rows loaded: {count}")
        print(
            f"Saved cursor: "
            f"{saved_cursor if saved_cursor else 'None'}"
        )

    finally:
        connection.close()


def validate_config(config_path):
    config = load_config(config_path)

    required_sections = [
        "source",
        "destination",
    ]

    for section in required_sections:
        if section not in config:
            raise ValueError(
                f"Missing required config section: {section}"
            )

    source = config["source"]
    destination = config["destination"]

    required_source = [
        "base_url",
        "endpoint",
    ]

    for field in required_source:
        if field not in source:
            raise ValueError(
                f"Missing source field: {field}"
            )

    required_destination = [
        "conn_env",
        "table",
        "primary_key",
    ]

    for field in required_destination:
        if field not in destination:
            raise ValueError(
                f"Missing destination field: {field}"
            )

    pagination_type = source.get(
        "pagination",
        {},
    ).get("type", "link_header")

    valid_pagination = {
        "link_header",
        "page",
        "cursor",
    }

    if pagination_type not in valid_pagination:
        raise ValueError(
            f"Invalid pagination type: {pagination_type}"
        )

    print("Configuration is valid.")


def main():
    parser = argparse.ArgumentParser(
        description="REST API to PostgreSQL bridge"
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    run_parser = subparsers.add_parser(
        "run",
        help="Fetch API data and load it into PostgreSQL",
    )

    run_parser.add_argument(
        "config",
        help="Path to YAML configuration file",
    )

    status_parser = subparsers.add_parser(
        "status",
        help="Show pipeline status",
    )

    status_parser.add_argument(
        "config",
        help="Path to YAML configuration file",
    )

    validate_parser = subparsers.add_parser(
        "validate",
        help="Validate configuration",
    )

    validate_parser.add_argument(
        "config",
        help="Path to YAML configuration file",
    )

    args = parser.parse_args()

    try:
        if args.command == "run":
            run_pipeline(args.config)

        elif args.command == "status":
            show_status(args.config)

        elif args.command == "validate":
            validate_config(args.config)

    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()