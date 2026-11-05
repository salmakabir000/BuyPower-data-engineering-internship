# api2sql - REST API to Postgres bridge

A reusable tool that pulls records from any REST API into a Postgres table.
The API, its pagination style, its rate limit and the destination table are all
described in a YAML config, so a new API means a new config file, not new code.

## Quick start

    pip install -r requirements.txt

    export PG_CONN=postgresql://postgres:password@localhost:5432/api2sql
    export GITHUB_TOKEN=...            # a read-only GitHub personal access token

    python3 api2sql.py run configs/github_issues.yml                 # incremental
    python3 api2sql.py run configs/github_issues.yml --full-refresh  # start over
    python3 api2sql.py run configs/github_issues.yml --lookback-window 7d
    python3 api2sql.py run configs/github_issues.yml --max-records 1000
    python3 api2sql.py status configs/github_issues.yml

Tests (26, using a mock API and a real Postgres): `python3 -m unittest test_api2sql -v`

## The config

    name: github_issues
    source:
      base_url: https://api.github.com
      endpoint: /repos/apache/airflow/issues
      params: {state: all, per_page: 100, sort: updated, direction: asc}
      auth: {type: bearer, token_env: GITHUB_TOKEN}
      pagination: {type: link_header}
      rate_limit: {requests_per_minute: 60}
      incremental: {cursor_field: updated_at, cursor_param: since, ascending: true}
    destination:
      conn_env: PG_CONN
      table: raw_github_issues
      primary_key: id
      write_mode: upsert

Other settings: `records_path` (where the list sits in the response, e.g. `results`),
`headers`, `flatten_depth` (default 2), `incremental.initial_lookback` (how far back the
first run starts, e.g. `7d`), and `auth` types `bearer`, `header` or `none`.

## Pagination strategies

| type | how the API says "there is more" | extra settings |
|---|---|---|
| `link_header` | a `Link: <...>; rel="next"` header (GitHub) | none |
| `next_cursor` | a field in the response body: a token, or a full URL | `cursor_path`, `cursor_param` |
| `page` | you ask for page 1, 2, 3... | `param`, `page_size` |
| `offset` | you ask to skip N records | `param`, `limit_param`, `page_size` |
| `none` | one request returns everything | none |

## Full refresh vs incremental

**Full refresh** ignores what was loaded before and fetches everything the API has. It is simple
and always correct, but it re-downloads data that has not changed, so it is slow and uses up
API quota on big sources. Use it for the first load, for small tables, when you suspect the
table has drifted from the source, or after a change to your config.

**Incremental** remembers how far it got (the "cursor", here the newest `updated_at` seen,
kept in the `_sync_state` table) and only asks the API for records changed after that point.
It is fast and light on the API, which is why it suits big, frequently-run loads. The
trade-off is that it can miss things: a record that arrives late with an old timestamp, or
a record deleted at the source, will never show up. `--lookback-window 7d` reduces the
first risk by re-fetching the last 7 days each time (the upsert makes the repeat harmless),
and an occasional `--full-refresh` repairs the rest.

Rule of thumb: full refresh for small or one-off data, incremental for large or regular data,
plus a periodic full refresh as a safety net.

## How the other requirements are met

- **Rate limiting:** requests are spaced evenly so no more than `requests_per_minute` are
  made; the tool also reads GitHub's `X-RateLimit-*` headers and waits for a reset if it is
  short, or stops with a clear message if it is not.
- **Retries:** HTTP 429, 5xx and connection errors are retried up to 5 times with
  exponential backoff (about 1s, 2s, 4s, 8s, 16s plus jitter). A `Retry-After` header is
  obeyed. Other errors (401, 404...) fail immediately with a hint.
- **Idempotent:** writes are `INSERT ... ON CONFLICT (primary_key) DO UPDATE`, so running
  twice never duplicates a row and changed records are updated in place.
- **Nested JSON to flat rows:** nested objects become columns (`user.login` becomes
  `user_login`) down to `flatten_depth`; lists and deeper objects are stored as JSONB.
  Column types are inferred (numbers, booleans, timestamps, JSONB, else text). If a field
  first appears on a later page, its column is added automatically.
- **Resumable:** if the API sorts by the cursor field, oldest first, set
  `incremental.ascending: true`. The cursor is then saved after every page, so a crash or
  `--max-records` stop resumes exactly where it left off. Without that setting the cursor is
  only advanced after a complete run, because stopping early could otherwise skip records.

## Known limitations

- The cursor is stored as of the last record seen, and APIs treat `since` as inclusive, so one
  boundary record is fetched again each run. The upsert makes this harmless.
- Records deleted at the source stay in the table.
- Columns are only ever added, never changed. If a field changes type later, the load fails
  with a database error rather than silently converting.
- GitHub's issues endpoint also returns pull requests (they have a `pull_request_*` column).
- Only `rest_api` sources, `postgres` destinations and `upsert` writes are supported.
