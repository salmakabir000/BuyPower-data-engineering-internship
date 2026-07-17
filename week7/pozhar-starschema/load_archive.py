import gzip
import json
import sys
import psycopg2
from datetime import datetime
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

# Cache to avoid repeated DB lookups
actor_cache = {}
repo_cache = {}
event_type_cache = {}

def get_or_create_event_type(event_type_name):
    if event_type_name in event_type_cache:
        return event_type_cache[event_type_name]
    cur.execute("SELECT event_type_sk FROM dim_event_type WHERE event_type_name = %s", (event_type_name,))
    row = cur.fetchone()
    if row:
        event_type_cache[event_type_name] = row[0]
        return row[0]
    cur.execute("INSERT INTO dim_event_type (event_type_name) VALUES (%s) RETURNING event_type_sk", (event_type_name,))
    sk = cur.fetchone()[0]
    event_type_cache[event_type_name] = sk
    return sk

def get_or_create_actor(actor):
    if actor['id'] in actor_cache:
        return actor_cache[actor['id']]
    cur.execute("SELECT actor_sk FROM dim_actor WHERE actor_id = %s", (actor['id'],))
    row = cur.fetchone()
    if row:
        actor_cache[actor['id']] = row[0]
        return row[0]
    cur.execute("""
        INSERT INTO dim_actor (actor_id, login, display_name, url)
        VALUES (%s, %s, %s, %s) RETURNING actor_sk
    """, (actor['id'], actor['login'], actor.get('display_login'), actor.get('url')))
    sk = cur.fetchone()[0]
    actor_cache[actor['id']] = sk
    return sk

def get_or_create_repo(repo):
    if repo['id'] in repo_cache:
        return repo_cache[repo['id']]
    cur.execute("SELECT repo_sk FROM dim_repo WHERE repo_id = %s", (repo['id'],))
    row = cur.fetchone()
    if row:
        repo_cache[repo['id']] = row[0]
        return row[0]
    cur.execute("""
        INSERT INTO dim_repo (repo_id, repo_name, repo_url)
        VALUES (%s, %s, %s) RETURNING repo_sk
    """, (repo['id'], repo['name'], repo.get('url')))
    sk = cur.fetchone()[0]
    repo_cache[repo['id']] = sk
    return sk

def load_file(filepath):
    loaded = 0
    skipped = 0

    print(f"Loading {filepath}...")

    with gzip.open(filepath, 'rt') as f:
        for line in f:
            try:
                event = json.loads(line.strip())
                event_id = event['id']
                event_type = event['type']
                actor = event['actor']
                repo = event['repo']
                created_at = datetime.fromisoformat(event['created_at'].replace('Z', '+00:00'))
                date_sk = int(created_at.strftime("%Y%m%d"))

                event_type_sk = get_or_create_event_type(event_type)
                actor_sk = get_or_create_actor(actor)
                repo_sk = get_or_create_repo(repo)

                cur.execute("""
                    INSERT INTO fact_events (event_id, event_type_sk, actor_sk, repo_sk, date_sk, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING
                """, (event_id, event_type_sk, actor_sk, repo_sk, date_sk, created_at))

                loaded += 1
                if loaded % 20000 == 0:
                    print(f"  {loaded:,} events processed...")

            except Exception as e:
                skipped += 1

    print(f"Finished {filepath}: Loaded {loaded:,} | Skipped {skipped:,}\n")
    return loaded, skipped

def main():
    files = sys.argv[1:]
    if not files:
        print("Usage: python3 load_archive.py file1.json.gz file2.json.gz ...")
        sys.exit(1)

    total_loaded = 0
    total_skipped = 0

    for filepath in files:
        loaded, skipped = load_file(filepath)
        total_loaded += loaded
        total_skipped += skipped

    print(f"=== ALL FILES DONE ===")
    print(f"Total loaded: {total_loaded:,} | Total skipped: {total_skipped:,}")

if __name__ == '__main__':
    main()
