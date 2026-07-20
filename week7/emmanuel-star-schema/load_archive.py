import gzip
import json
import psycopg2

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="postgres",
    user="postgres",
    password="password"
)

cursor = conn.cursor()

filename = "2024-01-15-12.json.gz"

count = 0

with gzip.open(filename, "rt", encoding="utf-8") as file:
    for line in file:
        event = json.loads(line)

        # Event Type
        event_type = event["type"]

        cursor.execute("""
            INSERT INTO dim_event_type (event_type_name)
            VALUES (%s)
            ON CONFLICT (event_type_name) DO NOTHING;
        """, (event_type,))

        # Actor
        actor = event["actor"]

        cursor.execute("""
            INSERT INTO dim_actor (actor_id, login, display_name, url)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (actor_id) DO NOTHING;
        """, (
            actor["id"],
            actor["login"],
            actor["login"],
            actor["url"]
        ))

        # Repository
        repo = event["repo"]

        cursor.execute("""
            INSERT INTO dim_repo (repo_id, repo_name, repo_url)
            VALUES (%s, %s, %s)
            ON CONFLICT (repo_id) DO NOTHING;
        """, (
            repo["id"],
            repo["name"],
            f"https://github.com/{repo['name']}"
        ))

        # Get surrogate keys
        cursor.execute(
            "SELECT actor_sk FROM dim_actor WHERE actor_id = %s",
            (actor["id"],)
        )
        actor_sk = cursor.fetchone()[0]

        cursor.execute(
            "SELECT repo_sk FROM dim_repo WHERE repo_id = %s",
            (repo["id"],)
        )
        repo_sk = cursor.fetchone()[0]

        cursor.execute(
            "SELECT event_type_sk FROM dim_event_type WHERE event_type_name = %s",
            (event_type,)
        )
        event_type_sk = cursor.fetchone()[0]

        # Date key
        created_at = event["created_at"]
        date_sk = int(created_at[:10].replace("-", ""))

        # Insert into fact_events
        cursor.execute("""
            INSERT INTO fact_events (
                event_id,
                event_type_sk,
                actor_sk,
                repo_sk,
                date_sk,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING;
        """, (
            event["id"],
            event_type_sk,
            actor_sk,
            repo_sk,
            date_sk,
            created_at
        ))

        count += 1

        if count == 100:
            break

conn.commit()

print(f"Loaded {count} events successfully!")

cursor.close()
conn.close()