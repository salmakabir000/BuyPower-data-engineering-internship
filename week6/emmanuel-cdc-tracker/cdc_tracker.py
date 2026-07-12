import psycopg2
import json

# Load watermark
with open("last_run.json", "r") as f:
    watermark = json.load(f)

last_updated = watermark["last_updated_at"]

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="postgres",
    user="postgres",
    password="password"
)

cursor = conn.cursor()

cursor.execute(
    "SELECT * FROM customers WHERE updated_at > %s ORDER BY updated_at ASC;",
    (last_updated,)
)

rows = cursor.fetchall()

latest = last_updated

with open("events.jsonl", "a") as events:
    for row in rows:
        if row[6] is not None:
            op = "DELETE"
        elif row[4] == row[5]:
            op = "INSERT"
        else:
            op = "UPDATE"

        event = {
            "op": op,
            "table": "customers",
            "ts": str(row[5]),
            "data": {
                "id": row[0],
                "name": row[1],
                "email": row[2],
                "city": row[3]
            }
        }

        events.write(json.dumps(event) + "\n")

        latest = str(row[5])

cursor.close()
conn.close()

with open("last_run.json", "w") as f:
    json.dump({"last_updated_at": latest}, f, indent=4)

print("CDC completed successfully.")