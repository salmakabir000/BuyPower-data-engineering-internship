import sqlite3
import pandas as pd


class sqlite:
    def __init__(self, config):
        self.config = config

    def load(self, data):
        path = self.config["path"]
        table = self.config["table"]
        on_conflict = self.config.get(
            "on_conflict",
            "append"
        )

        connection = sqlite3.connect(path)

        # Create the table if it does not exist yet
        data.head(0).to_sql(
            table,
            connection,
            if_exists="append",
            index=False
        )

        if on_conflict == "ignore":
            existing = pd.read_sql_query(
                f"SELECT * FROM {table}",
                connection
            )

            if "id" in data.columns and "id" in existing.columns:
                data = data[
                    ~data["id"].isin(existing["id"])
                ]

        if not data.empty:
            data.to_sql(
                table,
                connection,
                if_exists="append",
                index=False
            )

        connection.close()

        print(
            f"Saved {len(data)} rows to {table}"
        )


class parquet_file:
    def __init__(self, config):
        self.config = config

    def load(self, data):
        path = self.config["path"]

        data.to_parquet(
            path,
            index=False
        )

        print(
            f"Saved {len(data)} rows to {path}"
        )