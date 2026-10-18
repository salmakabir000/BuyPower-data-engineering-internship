import sqlite3


def write(df, config):
    database = config["database"]
    table = config["table"]

    connection = sqlite3.connect(database)

    try:
        df.to_sql(table, connection, if_exists="replace", index=False)
        print(f"Saved {len(df)} rows to {database}, table '{table}'")
    finally:
        connection.close()
