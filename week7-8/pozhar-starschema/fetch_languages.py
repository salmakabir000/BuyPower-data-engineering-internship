import psycopg2
import requests
import time

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="postgres",
    user="postgres",
    password="password"
)
cur = conn.cursor()

def get_top_20_actors():
    cur.execute("""
        SELECT a.login, COUNT(*) as issues_created
        FROM fact_events fe
        JOIN dim_event_type et ON fe.event_type_sk = et.event_type_sk
        JOIN dim_actor a ON fe.actor_sk = a.actor_sk
        WHERE et.event_type_name = 'IssuesEvent'
        GROUP BY a.login
        ORDER BY issues_created DESC
        LIMIT 20
    """)
    return [row[0] for row in cur.fetchall()]

def get_repos_for_actors(actors):
    cur.execute("""
        SELECT DISTINCT a.login, r.repo_name
        FROM fact_events fe
        JOIN dim_event_type et ON fe.event_type_sk = et.event_type_sk
        JOIN dim_actor a ON fe.actor_sk = a.actor_sk
        JOIN dim_repo r ON fe.repo_sk = r.repo_sk
        WHERE et.event_type_name = 'IssuesEvent'
        AND a.login = ANY(%s)
    """, (actors,))
    return cur.fetchall()

def get_language(repo_name):
    try:
        response = requests.get(f"https://api.github.com/repos/{repo_name}")
        if response.status_code == 200:
            return response.json().get('language', 'Unknown')
        elif response.status_code == 403:
            print("Rate limited, waiting 60s...")
            time.sleep(60)
            return get_language(repo_name)
        else:
            return 'Not Found'
    except Exception as e:
        return f"Error: {e}"

def main():
    actors = get_top_20_actors()
    print(f"Top 20 actors: {actors}\n")

    repos = get_repos_for_actors(actors)
    print(f"Found {len(repos)} actor-repo pairs\n")

    results = []
    for login, repo_name in repos:
        lang = get_language(repo_name)
        results.append((login, repo_name, lang))
        print(f"{login} | {repo_name} | {lang}")
        time.sleep(1)

    print("\n=== Summary ===")
    for login, repo_name, lang in results:
        print(f"{login}: {repo_name} -> {lang}")

if __name__ == '__main__':
    main()
