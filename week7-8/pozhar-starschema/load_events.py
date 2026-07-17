import json
import psycopg2
from datetime import datetime, date, timezone
import calendar

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="postgres",
    user="postgres",
    password="password"
)
conn.autocommit = True
cur = conn.cursor()

def get_or_create_event_type(event_type_name):
    cur.execute("SELECT event_type_sk FROM dim_event_type WHERE event_type_name = %s", (event_type_name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO dim_event_type (event_type_name) VALUES (%s) RETURNING event_type_sk", (event_type_name,))
    return cur.fetchone()[0]

def get_or_create_actor(actor):
    cur.execute("SELECT actor_sk FROM dim_actor WHERE actor_id = %s", (actor['id'],))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("""
        INSERT INTO dim_actor (actor_id, login, display_name, url)
        VALUES (%s, %s, %s, %s) RETURNING actor_sk
    """, (actor['id'], actor['login'], actor.get('display_login'), actor.get('url')))
    return cur.fetchone()[0]

def get_or_create_repo(repo):
    cur.execute("SELECT repo_sk FROM dim_repo WHERE repo_id = %s", (repo['id'],))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("""
        INSERT INTO dim_repo (repo_id, repo_name, repo_url)
        VALUES (%s, %s, %s) RETURNING repo_sk
    """, (repo['id'], repo['name'], repo.get('url')))
    return cur.fetchone()[0]

def get_or_create_date(dt):
    date_sk = int(dt.strftime("%Y%m%d"))
    cur.execute("SELECT date_sk FROM dim_date WHERE date_sk = %s", (date_sk,))
    if cur.fetchone():
        return date_sk
    cur.execute("""
        INSERT INTO dim_date (date_sk, date, year, quarter, month, month_name, day, day_of_week, day_name, is_weekend)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        date_sk,
        dt.date(),
        dt.year,
        (dt.month - 1) // 3 + 1,
        dt.month,
        calendar.month_name[dt.month],
        dt.day,
        dt.weekday(),
        calendar.day_name[dt.weekday()],
        dt.weekday() >= 5
    ))
    return date_sk

def load_events(filepath):
    loaded = 0
    skipped = 0

    with open(filepath) as f:
        for line in f:
            try:
                event = json.loads(line.strip())
                event_id = event['id']
                event_type = event['type']
                actor = event['actor']
                repo = event['repo']
                created_at = datetime.fromisoformat(event['created_at'].replace('Z', '+00:00'))

                event_type_sk = get_or_create_event_type(event_type)
                actor_sk = get_or_create_actor(actor)
                repo_sk = get_or_create_repo(repo)
                date_sk = get_or_create_date(created_at)

                cur.execute("""
                    INSERT INTO fact_events (event_id, event_type_sk, actor_sk, repo_sk, date_sk, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING
                """, (event_id, event_type_sk, actor_sk, repo_sk, date_sk, created_at))

                loaded += 1
                if loaded % 10000 == 0:
                    print(f"Loaded {loaded:,} events...")

            except Exception as e:
                skipped += 1

    print(f"\nDone! Loaded: {loaded:,} | Skipped: {skipped:,}")

if __name__ == '__main__':
    load_events("2024-01-15-12.json")
