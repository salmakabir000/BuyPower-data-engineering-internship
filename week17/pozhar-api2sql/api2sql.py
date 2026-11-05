#!/usr/bin/env python3
"""
api2sql.py - a generic bridge from any REST API into a Postgres table.

Usage:
    python3 api2sql.py run configs/github_issues.yml                  # incremental
    python3 api2sql.py run configs/github_issues.yml --full-refresh   # start over
    python3 api2sql.py run configs/github_issues.yml --lookback-window 7d
    python3 api2sql.py run configs/github_issues.yml --max-records 1000
    python3 api2sql.py status configs/github_issues.yml

Everything about the API (URL, auth, pagination style, rate limit) and the
table (name, primary key) lives in the YAML config, so a new API is a new
config file, not new code.
"""

import argparse
import json
import os
import random
import re
import sys
import time
from datetime import datetime, timedelta, timezone

import psycopg2
import requests
import yaml
from psycopg2 import sql
from psycopg2.extras import Json, execute_values

PAGINATION_TYPES = ("none", "link_header", "next_cursor", "page", "offset")
ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})$")
_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


class ConfigError(ValueError):
    """Something is wrong with the YAML config or the environment."""


class ApiError(RuntimeError):
    """The API failed in a way we can't recover from."""


class RateLimitExhausted(ApiError):
    """The API says we're out of requests and the wait is too long."""


# ---------------------------------------------------------------- small helpers

def dig(obj, path):
    """Follows a dotted path ('meta.next') into nested dicts; None if missing."""
    node = obj
    for key in str(path).split("."):
        if isinstance(node, dict) and key in node:
            node = node[key]
        else:
            return None
    return node


def parse_dt(value):
    """ISO timestamp string -> aware datetime, or None if it isn't one."""
    if not isinstance(value, str) or not ISO_RE.match(value):
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def format_dt(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_duration(text):
    m = re.fullmatch(r"\s*(\d+)\s*([smhdw])\s*", str(text).lower())
    if not m:
        raise ConfigError(f"Bad duration '{text}'. Use a number and a unit, like 30m, 12h or 7d.")
    return timedelta(seconds=int(m.group(1)) * _UNITS[m.group(2)])


def apply_lookback(cursor, delta):
    dt = parse_dt(cursor)
    if dt is None:
        raise ConfigError(
            f"--lookback-window only works when the cursor is a timestamp, but the stored cursor is '{cursor}'."
        )
    return format_dt(dt - delta)


def cursor_key(value):
    """Sort key so cursors compare correctly whether they are numbers, timestamps or text."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return (0, float(value))
    if isinstance(value, str):
        if re.fullmatch(r"-?\d+(\.\d+)?", value.strip()):
            return (0, float(value))
        dt = parse_dt(value)
        if dt is not None:
            return (1, dt.timestamp())
    return (2, str(value))


# ---------------------------------------------------------------- nested JSON -> flat rows

def sanitize_column(name):
    name = re.sub(r"[^a-z0-9_]", "_", str(name).lower())
    if not name or name[0].isdigit():
        name = "c_" + name
    return name[:63]


def flatten(record, max_depth=2):
    """{'user': {'login': 'ada'}} -> {'user_login': 'ada'}. Dicts nested deeper
    than max_depth, and all lists, are kept whole (stored as JSONB)."""
    out = {}

    def walk(node, prefix, depth):
        for key, value in node.items():
            col = sanitize_column(prefix + str(key))
            if isinstance(value, dict) and value and depth < max_depth:
                walk(value, col + "_", depth + 1)
            else:
                out[col] = value

    walk(record, "", 0)
    return out


def sql_type(values):
    """Picks a Postgres type that fits every non-null value seen for a column."""
    kinds = set()
    for v in values:
        if v is None:
            continue
        if isinstance(v, bool):
            kinds.add("BOOLEAN")
        elif isinstance(v, int):
            kinds.add("BIGINT")
        elif isinstance(v, float):
            kinds.add("DOUBLE PRECISION")
        elif isinstance(v, (dict, list)):
            kinds.add("JSONB")
        elif isinstance(v, str) and ISO_RE.match(v):
            kinds.add("TIMESTAMPTZ")
        else:
            kinds.add("TEXT")
    if not kinds:
        return "TEXT"
    if len(kinds) == 1:
        return next(iter(kinds))
    if kinds == {"BIGINT", "DOUBLE PRECISION"}:
        return "DOUBLE PRECISION"
    return "TEXT"


def _dumps(obj):
    return json.dumps(obj, default=str).replace("\\u0000", "")  # Postgres rejects NUL characters


# ---------------------------------------------------------------- config

def load_config(path):
    if not os.path.exists(path):
        raise ConfigError(f"Config file not found: {path}")
    try:
        with open(path) as f:
            cfg = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigError(f"{path} is not valid YAML: {e}") from e
    if not isinstance(cfg, dict):
        raise ConfigError(f"{path} must contain key: value settings at the top level")
    return validate_config(cfg, path)


def validate_config(cfg, path="<config>"):
    def need(d, key, where):
        if not isinstance(d, dict) or d.get(key) in (None, ""):
            raise ConfigError(f"{path}: missing required setting '{key}' in {where}")
        return d[key]

    need(cfg, "name", "the config")
    src = need(cfg, "source", "the config")
    dest = need(cfg, "destination", "the config")
    if src.get("type", "rest_api") != "rest_api":
        raise ConfigError(f"{path}: source type '{src.get('type')}' is not supported (use rest_api)")
    need(src, "base_url", "source")
    need(src, "endpoint", "source")

    # pagination / rate_limit / incremental belong under source, but accept them at the top level too
    for key in ("pagination", "rate_limit", "incremental"):
        if key not in src and key in cfg:
            src[key] = cfg[key]

    pag = src.setdefault("pagination", {"type": "none"})
    ptype = pag.get("type")
    if ptype not in PAGINATION_TYPES:
        raise ConfigError(f"{path}: unknown pagination type '{ptype}'. Supported: {', '.join(PAGINATION_TYPES)}")
    if ptype == "next_cursor":
        need(pag, "cursor_path", "pagination")
        pag.setdefault("cursor_param", "cursor")
    elif ptype == "page":
        pag.setdefault("param", "page")
    elif ptype == "offset":
        pag.setdefault("param", "offset")
        pag.setdefault("limit_param", "limit")
        need(pag, "page_size", "pagination")

    rl = src.get("rate_limit")
    if rl is not None:
        rpm = rl.get("requests_per_minute")
        if not isinstance(rpm, (int, float)) or rpm <= 0:
            raise ConfigError(f"{path}: rate_limit.requests_per_minute must be a positive number")

    inc = src.get("incremental")
    if inc is not None:
        need(inc, "cursor_field", "incremental")
        need(inc, "cursor_param", "incremental")
        if inc.get("initial_lookback"):
            parse_duration(inc["initial_lookback"])

    auth = src.get("auth") or {"type": "none"}
    atype = auth.get("type", "none")
    if atype == "bearer":
        need(auth, "token_env", "auth")
    elif atype == "header":
        need(auth, "name", "auth")
        need(auth, "value_env", "auth")
    elif atype != "none":
        raise ConfigError(f"{path}: unknown auth type '{atype}' (use bearer, header or none)")
    src["auth"] = auth

    if dest.get("type", "postgres") != "postgres":
        raise ConfigError(f"{path}: destination type '{dest.get('type')}' is not supported (use postgres)")
    need(dest, "conn_env", "destination")
    need(dest, "table", "destination")
    pk = need(dest, "primary_key", "destination")
    dest["primary_key"] = [pk] if isinstance(pk, str) else list(pk)
    if dest.setdefault("write_mode", "upsert") != "upsert":
        raise ConfigError(f"{path}: write_mode '{dest['write_mode']}' is not supported (use upsert)")
    return cfg


def build_headers(source):
    headers = {"Accept": "application/json", "User-Agent": "api2sql/1.0"}
    headers.update(source.get("headers") or {})
    auth = source.get("auth") or {}
    if auth.get("type") == "bearer":
        token = os.environ.get(auth["token_env"])
        if not token:
            raise ConfigError(f"Environment variable {auth['token_env']} is not set (it should hold your API token)")
        headers["Authorization"] = f"Bearer {token}"
    elif auth.get("type") == "header":
        value = os.environ.get(auth["value_env"])
        if not value:
            raise ConfigError(f"Environment variable {auth['value_env']} is not set")
        headers[auth["name"]] = value
    return headers


# ---------------------------------------------------------------- HTTP: rate limiting + retries

class RateLimiter:
    """Caps requests per minute by spacing them evenly (60/min = one per second)."""

    def __init__(self, per_minute, clock=time.monotonic, sleep=time.sleep):
        self.interval = 60.0 / per_minute if per_minute else 0.0
        self.clock = clock
        self.sleep = sleep
        self.next_allowed = 0.0

    def wait(self):
        if not self.interval:
            return
        now = self.clock()
        if self.next_allowed > now:
            self.sleep(self.next_allowed - now)
            now = self.clock()
        self.next_allowed = now + self.interval


class Client:
    def __init__(self, source, sleep=time.sleep, clock=time.time, session=None,
                 backoff_base=1.0, max_retries=5, max_wait=120.0):
        self.session = session or requests.Session()
        self.sleep = sleep
        self.clock = clock
        self.backoff_base = backoff_base
        self.max_retries = max_retries
        self.max_wait = max_wait
        self.timeout = source.get("timeout_seconds", 30)
        self.headers = build_headers(source)
        rpm = (source.get("rate_limit") or {}).get("requests_per_minute")
        self.limiter = RateLimiter(rpm, sleep=sleep)
        self.blocked_until = None  # epoch seconds, set when the API says we're out of requests
        self.requests_made = 0

    def get(self, url, params=None):
        self._wait_out_rate_limit()
        attempt = 0
        while True:
            self.limiter.wait()
            self.requests_made += 1
            resp, problem = None, None
            try:
                resp = self.session.get(url, params=params, headers=self.headers, timeout=self.timeout)
            except (requests.ConnectionError, requests.Timeout) as e:
                problem = type(e).__name__
            else:
                status = resp.status_code
                if status < 400:
                    self._note_rate_headers(resp)
                    return resp
                if status == 429 or status >= 500 or (status == 403 and self._looks_rate_limited(resp)):
                    problem = f"HTTP {status}"
                else:
                    raise ApiError(self._describe(resp))

            if attempt >= self.max_retries:
                raise ApiError(f"Giving up after {self.max_retries} retries ({problem}) for {url}")
            wait = self._backoff(resp, attempt)
            attempt += 1
            print(f"  retry {attempt}/{self.max_retries} in {wait:.1f}s ({problem})")
            self.sleep(wait)

    def _backoff(self, resp, attempt):
        """Seconds to wait before retrying: the API's own hint if it gave one,
        otherwise exponential backoff (1s, 2s, 4s, 8s, 16s) plus a little jitter."""
        if resp is not None:
            wait = None
            retry_after = resp.headers.get("Retry-After", "").strip()
            if retry_after.isdigit():
                wait = float(retry_after)
            elif resp.headers.get("X-RateLimit-Remaining") == "0" and resp.headers.get("X-RateLimit-Reset"):
                wait = float(resp.headers["X-RateLimit-Reset"]) - self.clock() + 1
            if wait is not None:
                if wait > self.max_wait:
                    raise RateLimitExhausted(
                        f"The API asked us to wait {int(wait)}s, which is longer than our {int(self.max_wait)}s limit. "
                        "Use an API token (higher limits) or try again later."
                    )
                return max(wait, 0.0)
        return self.backoff_base * (2 ** attempt) + random.uniform(0, self.backoff_base)

    def _looks_rate_limited(self, resp):
        return resp.headers.get("X-RateLimit-Remaining") == "0" or "Retry-After" in resp.headers

    def _note_rate_headers(self, resp):
        remaining = resp.headers.get("X-RateLimit-Remaining")
        reset = resp.headers.get("X-RateLimit-Reset")
        self.blocked_until = float(reset) if remaining == "0" and reset else None

    def _wait_out_rate_limit(self):
        if self.blocked_until is None:
            return
        delay = self.blocked_until - self.clock()
        if delay > self.max_wait:
            raise RateLimitExhausted(
                f"API request limit used up; it resets in {int(delay)}s. "
                "Use an API token (higher limits) or try again later."
            )
        if delay > 0:
            print(f"  API limit reached, waiting {delay:.0f}s for it to reset")
            self.sleep(delay + 1)
        self.blocked_until = None

    @staticmethod
    def _describe(resp):
        hint = {401: " Check your API token.", 403: " Check your token's permissions.", 404: " Check base_url and endpoint."}
        return f"HTTP {resp.status_code} from {resp.url}: {resp.text[:200]!r}.{hint.get(resp.status_code, '')}"


# ---------------------------------------------------------------- pagination

def extract_records(body, records_path):
    node = body
    if records_path:
        for key in records_path.split("."):
            if isinstance(node, dict) and key in node:
                node = node[key]
            else:
                raise ApiError(f"records_path '{records_path}' was not found in the API response")
    if not isinstance(node, list):
        raise ApiError(
            f"Expected a list of records but the API returned a {type(node).__name__}. "
            "Set source.records_path to point at the list."
        )
    return node


def iter_pages(client, source, extra_params):
    """Yields one list of records per API page, following the configured pagination style."""
    pag = source["pagination"]
    kind = pag["type"]
    url = source["base_url"].rstrip("/") + "/" + source["endpoint"].lstrip("/")
    params = {**(source.get("params") or {}), **extra_params}
    rec_path = source.get("records_path")
    max_pages = pag.get("max_pages", 1_000_000)

    if kind == "none":
        yield extract_records(client.get(url, params).json(), rec_path)

    elif kind == "link_header":
        # GitHub style: the Link header carries the full URL of the next page
        resp = client.get(url, params)
        for _ in range(max_pages):
            yield extract_records(resp.json(), rec_path)
            next_url = resp.links.get("next", {}).get("url")
            if not next_url:
                break
            resp = client.get(next_url)  # the next URL already contains every query parameter

    elif kind == "next_cursor":
        # the response body holds a token (or a full URL) pointing at the next page
        cursor, seen = None, set()
        for _ in range(max_pages):
            if isinstance(cursor, str) and cursor.startswith(("http://", "https://")):
                body = client.get(cursor).json()
            else:
                page_params = dict(params)
                if cursor is not None:
                    page_params[pag["cursor_param"]] = cursor
                body = client.get(url, page_params).json()
            yield extract_records(body, rec_path)
            cursor = dig(body, pag["cursor_path"])
            if not cursor or cursor in seen:
                break
            seen.add(cursor)

    elif kind == "page":
        page_size = pag.get("page_size")
        for page in range(pag.get("start", 1), pag.get("start", 1) + max_pages):
            page_params = {**params, pag["param"]: page}
            if page_size and pag.get("page_size_param"):
                page_params[pag["page_size_param"]] = page_size
            records = extract_records(client.get(url, page_params).json(), rec_path)
            if not records:
                break
            yield records
            if page_size and len(records) < page_size:
                break

    elif kind == "offset":
        page_size = pag["page_size"]
        offset = pag.get("start", 0)
        for _ in range(max_pages):
            page_params = {**params, pag["param"]: offset, pag["limit_param"]: page_size}
            records = extract_records(client.get(url, page_params).json(), rec_path)
            if not records:
                break
            yield records
            if len(records) < page_size:
                break
            offset += page_size


# ---------------------------------------------------------------- Postgres

def ident(table):
    return sql.Identifier(*table.split("."))


class PostgresWriter:
    """Creates the table from the data, adds columns that show up later, and upserts."""

    def __init__(self, conn, table, primary_key):
        self.conn = conn
        self.table = table
        self.pk = primary_key
        self.types = None  # column -> postgres data_type, loaded from information_schema

    def _load_columns(self):
        schema, _, name = self.table.rpartition(".")
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = COALESCE(NULLIF(%s, ''), current_schema()) AND table_name = %s",
                (schema, name),
            )
            self.types = {c: t for c, t in cur.fetchall()}
        self.conn.commit()

    def upsert(self, rows):
        if not rows:
            return 0
        for row in rows:
            for key in self.pk:
                if row.get(key) is None:
                    raise ApiError(f"A record has no value for primary key '{key}' - check destination.primary_key")
        # if one page repeats a record, keep the last copy (Postgres rejects duplicates in one statement)
        rows = list({tuple(r[k] for k in self.pk): r for r in rows}.values())

        cols = list(self.pk)
        for row in rows:
            for c in row:
                if c not in cols and c != "_synced_at":
                    cols.append(c)

        if self.types is None:
            self._load_columns()
        if not self.types:
            self._create_table(cols, rows)
        else:
            self._add_missing_columns(cols, rows)

        tuples = [tuple(self._adapt(c, r.get(c)) for c in cols) for r in rows]
        updates = [sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in cols if c not in self.pk]
        updates.append(sql.SQL("_synced_at = now()"))
        query = sql.SQL("INSERT INTO {t} ({cols}) VALUES %s ON CONFLICT ({pk}) DO UPDATE SET {upd}").format(
            t=ident(self.table),
            cols=sql.SQL(", ").join(map(sql.Identifier, cols)),
            pk=sql.SQL(", ").join(map(sql.Identifier, self.pk)),
            upd=sql.SQL(", ").join(updates),
        )
        with self.conn.cursor() as cur:
            execute_values(cur, query.as_string(cur), tuples, page_size=500)
        self.conn.commit()
        return len(rows)

    def _column_def(self, col, rows):
        return sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(sql_type(r.get(col) for r in rows)))

    def _create_table(self, cols, rows):
        defs = [self._column_def(c, rows) for c in cols]
        defs.append(sql.SQL("_synced_at TIMESTAMPTZ NOT NULL DEFAULT now()"))
        defs.append(sql.SQL("PRIMARY KEY ({})").format(sql.SQL(", ").join(map(sql.Identifier, self.pk))))
        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE TABLE {} ({})").format(ident(self.table), sql.SQL(", ").join(defs)))
        self.conn.commit()
        self._load_columns()

    def _add_missing_columns(self, cols, rows):
        missing = [c for c in cols if c not in self.types]
        if not missing:
            return
        with self.conn.cursor() as cur:
            for c in missing:
                cur.execute(sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {}").format(
                    ident(self.table), self._column_def(c, rows)))
        self.conn.commit()
        self._load_columns()

    def _adapt(self, col, value):
        """Converts a Python value to what the column's Postgres type expects."""
        if value is None:
            return None
        if self.types.get(col) == "jsonb":
            return Json(value, dumps=_dumps)
        if isinstance(value, (dict, list)):
            return _dumps(value)  # structure landing in a text column
        if isinstance(value, str):
            return value.replace("\x00", "")
        return value

    def count(self):
        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("SELECT count(*) FROM {}").format(ident(self.table)))
            n = cur.fetchone()[0]
        self.conn.commit()
        return n

    def exists(self):
        if self.types is None:
            self._load_columns()
        return bool(self.types)


STATE_DDL = (
    "CREATE TABLE IF NOT EXISTS _sync_state ("
    "name TEXT PRIMARY KEY, cursor_field TEXT, cursor_value TEXT, "
    "last_sync_at TIMESTAMPTZ, last_run_rows INTEGER, last_run_mode TEXT)"
)


def ensure_state_table(conn):
    with conn.cursor() as cur:
        cur.execute(STATE_DDL)
    conn.commit()


def get_state(conn, name):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT cursor_field, cursor_value, last_sync_at, last_run_rows, last_run_mode "
            "FROM _sync_state WHERE name = %s", (name,))
        row = cur.fetchone()
    conn.commit()
    if not row:
        return None
    return dict(zip(("cursor_field", "cursor_value", "last_sync_at", "last_run_rows", "last_run_mode"), row))


def save_state(conn, name, cursor_field, cursor_value, rows, mode):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO _sync_state (name, cursor_field, cursor_value, last_sync_at, last_run_rows, last_run_mode) "
            "VALUES (%s, %s, %s, now(), %s, %s) "
            "ON CONFLICT (name) DO UPDATE SET cursor_field = EXCLUDED.cursor_field, "
            "cursor_value = EXCLUDED.cursor_value, last_sync_at = now(), "
            "last_run_rows = EXCLUDED.last_run_rows, last_run_mode = EXCLUDED.last_run_mode",
            (name, cursor_field, None if cursor_value is None else str(cursor_value), rows, mode))
    conn.commit()


# ---------------------------------------------------------------- the sync itself

def sync(cfg, conn, full_refresh=False, lookback=None, max_records=None, client=None, now=None):
    src, dest, name = cfg["source"], cfg["destination"], cfg["name"]
    inc = src.get("incremental")
    ensure_state_table(conn)
    state = get_state(conn, name)
    stored = state["cursor_value"] if state else None

    cursor = None
    if inc and not full_refresh:
        if stored is not None:
            cursor = apply_lookback(stored, parse_duration(lookback)) if lookback else stored
        elif inc.get("initial_lookback"):
            start = (now or datetime.now(timezone.utc)) - parse_duration(inc["initial_lookback"])
            cursor = format_dt(start)

    if full_refresh or not inc:
        mode = "full-refresh"
    else:
        mode = "incremental" if stored is not None else "initial"
    extra = {inc["cursor_param"]: cursor} if inc and cursor is not None else {}
    print(f"Syncing '{name}' ({mode})" + (f", fetching records with {inc['cursor_param']}={cursor}" if extra else ""))

    client = client or Client(src)
    writer = PostgresWriter(conn, dest["table"], dest["primary_key"])
    depth = src.get("flatten_depth", 2)
    ascending = bool(inc and inc.get("ascending"))

    total, pages, max_cursor, stopped_early = 0, 0, None, False
    for page in iter_pages(client, src, extra):
        if max_records is not None and total + len(page) > max_records:
            page = page[: max_records - total]
            stopped_early = True
        if not page:
            break
        writer.upsert([flatten(r, depth) for r in page])
        total += len(page)
        pages += 1
        print(f"  page {pages}: {len(page)} records (total {total})")

        if inc:
            for record in page:
                value = dig(record, inc["cursor_field"])
                if value is not None and (max_cursor is None or cursor_key(value) > cursor_key(max_cursor)):
                    max_cursor = value
            if ascending and max_cursor is not None:
                # records arrive oldest-first, so progress so far is a safe place to resume from
                save_state(conn, name, inc["cursor_field"], max_cursor, total, mode)
        if stopped_early or (max_records is not None and total >= max_records):
            stopped_early = True
            break

    new_cursor = stored
    if inc and max_cursor is not None:
        advance = (not stopped_early) or ascending
        if advance and (full_refresh or stored is None or cursor_key(max_cursor) >= cursor_key(stored)):
            new_cursor = max_cursor
        elif stopped_early and not ascending:
            print("  Note: stopped early, and this API doesn't return oldest-first, so the cursor was NOT advanced "
                  "(otherwise the records we skipped could be missed). Set incremental.ascending: true only if "
                  "your API sorts by the cursor field, oldest first.")
    save_state(conn, name, inc["cursor_field"] if inc else None, new_cursor, total, mode)
    print(f"Done: {total} records upserted into {dest['table']}" + (f"; cursor is now {new_cursor}" if inc else ""))
    return {"mode": mode, "rows": total, "pages": pages, "cursor": new_cursor, "stopped_early": stopped_early}


def show_status(cfg, conn, out=print):
    ensure_state_table(conn)
    state = get_state(conn, cfg["name"])
    writer = PostgresWriter(conn, cfg["destination"]["table"], cfg["destination"]["primary_key"])
    inc = cfg["source"].get("incremental")
    out(f"Pipeline: {cfg['name']}  ->  table {cfg['destination']['table']}")
    if not state:
        out("Last sync: never")
    else:
        when = state["last_sync_at"].strftime("%Y-%m-%d %H:%M:%S %Z") if state["last_sync_at"] else "unknown"
        out(f"Last sync: {when} ({state['last_run_mode']}, {state['last_run_rows']} records that run)")
        if inc:
            out(f"Cursor ({inc['cursor_field']}): {state['cursor_value']}")
    out(f"Rows in table: {writer.count() if writer.exists() else '(table not created yet)'}")


# ---------------------------------------------------------------- command line

def connect(cfg):
    env = cfg["destination"]["conn_env"]
    dsn = os.environ.get(env)
    if not dsn:
        raise ConfigError(f"Environment variable {env} is not set. Example: export {env}=postgresql://user:password@localhost:5432/dbname")
    return psycopg2.connect(dsn, connect_timeout=10)


def main(argv=None):
    parser = argparse.ArgumentParser(prog="api2sql", description="Ingest a REST API into Postgres.")
    sub = parser.add_subparsers(dest="command", required=True)
    run_p = sub.add_parser("run", help="Sync the API into the table")
    run_p.add_argument("config")
    run_p.add_argument("--full-refresh", action="store_true", help="ignore the saved cursor and fetch everything")
    run_p.add_argument("--lookback-window", metavar="7d", help="re-fetch this much history before the cursor (incremental only)")
    run_p.add_argument("--max-records", type=int, metavar="N", help="stop after N records")
    status_p = sub.add_parser("status", help="Show last sync time and row count")
    status_p.add_argument("config")
    args = parser.parse_args(argv)

    conn = None
    try:
        cfg = load_config(args.config)
        conn = connect(cfg)
        if args.command == "run":
            sync(cfg, conn, args.full_refresh, args.lookback_window, args.max_records)
        else:
            show_status(cfg, conn)
        return 0
    except ConfigError as e:
        print(f"Config error: {e}", file=sys.stderr)
        return 2
    except ApiError as e:
        print(f"API error: {e}", file=sys.stderr)
        return 1
    except psycopg2.OperationalError as e:
        print(f"Couldn't use Postgres: {str(e).strip()}\nIs it running? Try: sudo docker start pg-cdc", file=sys.stderr)
        return 1
    except psycopg2.Error as e:
        print(f"Database error: {str(e).strip()}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nInterrupted. Pages already fetched were saved.", file=sys.stderr)
        return 130
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())
