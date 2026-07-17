# GitHub Events Star Schema

## What is a Star Schema?
A star schema organizes data into one central fact table 
surrounded by dimension tables — like a star shape. The fact 
table holds measurable events, while dimensions hold the 
descriptive details.

Think of it like a receipt:
- Fact table = the receipt (what happened, when)
- Dimensions = the details (who, where, what type)

## Tables

### fact_events (286,864 rows)
One row per GitHub event. Links to all dimension tables 
via surrogate keys.

### dim_actor
One row per unique GitHub user. 
Surrogate key (actor_sk) used instead of GitHub's id 
to protect against upstream changes.

### dim_repo
One row per unique repository.

### dim_event_type
15 rows — one per GitHub event type (PushEvent, 
PullRequestEvent, IssuesEvent etc.)

### dim_date
One row per calendar date with year, quarter, month, 
day, day name and weekend flag.

## Surrogate Keys vs Natural Keys
- Natural key = the real ID from the source (GitHub's event id)
- Surrogate key = our own auto-increment ID we create

We use surrogate keys because:
- Source IDs can change or be reused
- Joins are faster on integers than strings
- We control our own keys independently of the source

## Key Findings from the Data
- 286,864 events loaded from one hour of GitHub activity
- PushEvent = 200,257 (70% of all activity)
- github-actions[bot] made 64,298 events — most active "user"
- Accounts like ion561sdag and inse2233tto pushed 17,000+ 
  events in one hour — likely automated bots
- Real projects have meaningful names; numbered sequences 
  like "Projcts6", "Projcts15" are a bot signal

## Bot Detection Observation
Top repos by event count had suspicious patterns:
- Sequential names (Projcts6, Projcts15, Projcts17)
- Same user owning multiple numbered repos
- 2,000+ events per hour per repo
This is a real data quality issue in GitHub Archive data.

## How to run
1. Start Postgres: sudo docker start pg-cdc
2. Load schema: sudo docker exec -i pg-cdc psql -U postgres < schema.sql
3. Load data: python3 load_events.py

## Updated Results (3 Days of Data)
- Total events loaded: 836,573 across 3 days (Jan 15-17, 2024)
- 0 duplicates — deduplication via ON CONFLICT DO NOTHING works correctly

## Top 10 Actors by Issues Created
1. github-actions[bot] - 227 issues
2. kinjalsoftnoesis - 74 issues
3. poornima-krishnasamy - 65 issues
4. SAPDocs - 64 issues
5. linkspreed - 43 issues
6. juzeppej0stko - 37 issues
7. zspz - 35 issues
8. lisabenedetti - 32 issues
9. maxokon - 23 issues
10. softvision-raul-bucata - 23 issues

## Query Used
SELECT a.login, COUNT(*) as issues_created
FROM fact_events fe
JOIN dim_event_type et ON fe.event_type_sk = et.event_type_sk
JOIN dim_actor a ON fe.actor_sk = a.actor_sk
WHERE et.event_type_name = 'IssuesEvent'
GROUP BY a.login
ORDER BY issues_created DESC
LIMIT 10;

## Note on Repo Language
GitHub Archive's standard event payload does not include repo 
language directly. Getting language data would require an 
additional call to GitHub's REST API per repository, which was 
outside the scope of this week's data (event stream only).

## Unknown Row Pattern (Stretch Goal)
Added a row with surrogate key -1 to each dimension table 
representing "unknown" values. This is a real-world pattern 
used so fact rows with missing or unmatched dimension data 
can still load successfully instead of failing or being 
dropped. For example, if an actor record is corrupted or 
missing, the fact row can point to actor_sk = -1 instead 
of causing a foreign key violation.

## Load Script
load_archive.py reads one or more .json.gz files directly 
without needing manual unzipping. It uses in-memory caching 
for actors, repos and event types to avoid repeated database 
lookups, making the load significantly faster. Deduplication 
is handled via ON CONFLICT (event_id) DO NOTHING.
