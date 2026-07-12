import yaml
import sqlite3
import pandas as pd

with open("checks/coin_prices.yml", "r") as file:
    config = yaml.safe_load(file)

db_path = config["source"]["path"]
table_name = config["table"]

conn = sqlite3.connect(db_path)

df = pd.read_sql_query(
    f"SELECT * FROM {table_name}",
    conn
)

conn.close()

print(f"\nData Quality Report - {table_name}")
print(f"Total rows checked: {len(df)}\n")

critical_failures = 0

for check in config["checks"]:

    if check["type"] == "not_null":
        column = check["column"]
        failures = df[column].isnull().sum()

        print(f"{column} not_null: {failures} failures")

        if failures > 0 and check["severity"] == "critical":
            critical_failures += 1

    elif check["type"] == "unique":
        column = check["column"]
        failures = df.duplicated(subset=[column]).sum()

        print(f"{column} unique: {failures} duplicates")

        if failures > 0 and check["severity"] == "critical":
            critical_failures += 1

    elif check["type"] == "range":
        column = check["column"]
        minimum = check["min"]
        maximum = check["max"]

        failures = ((df[column] < minimum) | (df[column] > maximum)).sum()

        print(f"{column} range: {failures} failures")

        if failures > 0 and check["severity"] == "critical":
            critical_failures += 1

    elif check["type"] == "regex_match":
        column = check["column"]

        failures = (
            ~df[column]
            .astype(str)
            .str.match(check["pattern"])
        ).sum()

        print(f"{column} regex_match: {failures} failures")

        if failures > 0 and check["severity"] == "critical":
            critical_failures += 1

    elif check["type"] == "row_count_min":
        minimum = check["value"]

        if len(df) >= minimum:
            print(f"row_count_min passed ({len(df)} >= {minimum})")
        else:
            print(f"row_count_min failed ({len(df)} < {minimum})")

            if check["severity"] == "critical":
                critical_failures += 1

    elif check["type"] == "in_set":
        column = check["column"]
        allowed_values = check["values"]

        failures = (~df[column].isin(allowed_values)).sum()

        print(f"{column} in_set: {failures} failures")

        if failures > 0 and check["severity"] == "critical":
            critical_failures += 1

print("\n--------------------------------")

if critical_failures > 0:
    print(f"Result: FAILED ({critical_failures} critical failures)")
    exit(1)
else:
    print("Result: PASSED")
    exit(0)     