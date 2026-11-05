"""
Tests for api2sql.py. Unit tests need nothing; integration tests need Postgres:

    export PG_CONN=postgresql://postgres:password@localhost:5432/api2sql
    python3 -m unittest test_api2sql -v

The "API" is a small HTTP server running inside the test, so every pagination
style, rate-limit header and failure can be simulated reliably.
"""

import contextlib
import io
import json
import os
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlencode, urlparse

import psycopg2
import yaml

import api2sql

PG_CONN = os.environ.get("PG_CONN")
BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def iso(minutes):
    return api2sql.format_dt(BASE + timedelta(minutes=minutes))


def make_records(n=25):
    recs = []
    for i in range(1, n + 1):
        r = {"id": i, "title": f"issue {i}", "updated_at": iso(i), "user": {"login": f"user{i}", "id": 100 + i},
             "labels": [{"name": "bug"}], "assignee": None}
        if i > 15:
            r["milestone_title"] = "v2"  # a field that only shows up on later pages
        recs.append(r)
    return recs


class MockAPI:
    """A tiny REST API with five pagination styles and switchable failures."""

    def __init__(self, records, style="link", per_page=10):
        self.records, self.style, self.per_page = records, style, per_page
        self.log, self.failures, self.extra_headers = [], [], {}
        api = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_GET(self):
                parsed = urlparse(self.path)
                q = {k: v[0] for k, v in parse_qs(parsed.query).items()}
                api.log.append((parsed.path, q))
                if api.failures:
                    status, headers = api.failures.pop(0)
                    self.send_response(status)
                    for k, v in headers.items():
                        self.send_header(k, v)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                status, headers, body = api.respond(q)
                payload = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                for k, v in {**headers, **api.extra_headers}.items():
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(payload)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.base_url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def respond(self, q):
        data = sorted(self.records, key=lambda r: r["updated_at"])
        if "since" in q:
            data = [r for r in data if r["updated_at"] >= q["since"]]
        pp = self.per_page
        if self.style == "link":
            page = int(q.get("page", 1))
            chunk = data[(page - 1) * pp: page * pp]
            headers = {}
            if page * pp < len(data):
                headers["Link"] = f'<{self.base_url}/items?{urlencode({**q, "page": page + 1})}>; rel="next"'
            return 200, headers, chunk
        if self.style in ("cursor", "cursor_url"):
            start = int(q.get("cursor", 0)) if self.style == "cursor" else int(q.get("start", 0))
            chunk = data[start: start + pp]
            more = start + pp < len(data)
            if self.style == "cursor":
                return 200, {}, {"data": chunk, "meta": {"next_cursor": str(start + pp) if more else None}}
            nxt = f"{self.base_url}/items?{urlencode({**q, 'start': start + pp})}" if more else None
            return 200, {}, {"results": chunk, "next": nxt}
        if self.style == "page":
            page = int(q.get("page", 1))
            return 200, {}, data[(page - 1) * pp: page * pp]
        if self.style == "offset":
            off = int(q.get("offset", 0))
            return 200, {}, data[off: off + int(q.get("limit", pp))]
        raise AssertionError(self.style)

    def stop(self):
        self.server.shutdown()
        self.server.server_close()


PAGINATION = {
    "link": ({"type": "link_header"}, None),
    "cursor": ({"type": "next_cursor", "cursor_path": "meta.next_cursor", "cursor_param": "cursor"}, "data"),
    "cursor_url": ({"type": "next_cursor", "cursor_path": "next"}, "results"),
    "page": ({"type": "page", "param": "page", "page_size": 10, "page_size_param": "per_page"}, None),
    "offset": ({"type": "offset", "param": "offset", "limit_param": "limit", "page_size": 10}, None),
}


def make_cfg(api, table, incremental=True, ascending=False, name=None, initial_lookback=None):
    pagination, records_path = PAGINATION[api.style]
    src = {"type": "rest_api", "base_url": api.base_url, "endpoint": "/items", "pagination": dict(pagination),
           "params": {"per_page": 10}}
    if records_path:
        src["records_path"] = records_path
    if incremental:
        src["incremental"] = {"cursor_field": "updated_at", "cursor_param": "since", "ascending": ascending}
        if initial_lookback:
            src["incremental"]["initial_lookback"] = initial_lookback
    cfg = {"name": name or table, "source": src,
           "destination": {"type": "postgres", "conn_env": "PG_CONN", "table": table, "primary_key": "id"}}
    return api2sql.validate_config(cfg)


def quiet(fn, *a, **k):
    with contextlib.redirect_stdout(io.StringIO()):
        return fn(*a, **k)


# ======================================================================= unit tests (no database)

class UnitTests(unittest.TestCase):
    def test_parse_duration(self):
        self.assertEqual(api2sql.parse_duration("7d"), timedelta(days=7))
        self.assertEqual(api2sql.parse_duration("12h"), timedelta(hours=12))
        self.assertEqual(api2sql.parse_duration("30m"), timedelta(minutes=30))
        with self.assertRaises(api2sql.ConfigError):
            api2sql.parse_duration("a week")

    def test_lookback_moves_cursor_back(self):
        self.assertEqual(api2sql.apply_lookback("2026-01-10T00:00:00Z", timedelta(days=7)), "2026-01-03T00:00:00Z")
        with self.assertRaises(api2sql.ConfigError):
            api2sql.apply_lookback("12345", timedelta(days=1))

    def test_cursor_ordering(self):
        k = api2sql.cursor_key
        self.assertLess(k("2026-01-01T00:00:00Z"), k("2026-01-02T00:00:00Z"))
        self.assertLess(k("9"), k("10"))          # numbers compare as numbers, not text
        self.assertEqual(k("2026-01-01T01:00:00+01:00"), k("2026-01-01T00:00:00Z"))

    def test_flatten_nested_json(self):
        flat = api2sql.flatten({"id": 1, "user": {"login": "ada", "org": {"name": "x", "deep": {"a": 1}}},
                                "labels": [{"n": 1}], "Odd Key!": 2, "empty": {}}, max_depth=2)
        self.assertEqual(flat["user_login"], "ada")
        self.assertEqual(flat["user_org_name"], "x")
        self.assertEqual(flat["user_org_deep"], {"a": 1})   # deeper than max_depth stays whole
        self.assertEqual(flat["labels"], [{"n": 1}])        # lists stay whole
        self.assertEqual(flat["odd_key_"], 2)               # column names made safe
        self.assertEqual(flat["empty"], {})

    def test_type_inference(self):
        t = api2sql.sql_type
        self.assertEqual(t([1, 2, None]), "BIGINT")
        self.assertEqual(t([1, 2.5]), "DOUBLE PRECISION")
        self.assertEqual(t([True, None]), "BOOLEAN")
        self.assertEqual(t(["2026-01-01T00:00:00Z"]), "TIMESTAMPTZ")
        self.assertEqual(t([{"a": 1}, [1]]), "JSONB")
        self.assertEqual(t([None, None]), "TEXT")
        self.assertEqual(t([1, "x"]), "TEXT")               # mixed types fall back to text

    def test_rate_limiter_spaces_requests(self):
        t, sleeps = [0.0], []

        def sleep(s):
            sleeps.append(round(s, 3))
            t[0] += s

        rl = api2sql.RateLimiter(60, clock=lambda: t[0], sleep=sleep)
        for _ in range(5):
            rl.wait()
        self.assertEqual(sleeps, [1.0, 1.0, 1.0, 1.0])      # 60/min = one request per second
        t2, s2 = [0.0], []
        rl2 = api2sql.RateLimiter(120, clock=lambda: t2[0], sleep=lambda s: (s2.append(round(s, 3)), t2.__setitem__(0, t2[0] + s)))
        for _ in range(3):
            rl2.wait()
        self.assertEqual(s2, [0.5, 0.5])

    def test_config_validation_fails_clearly(self):
        good = {"name": "x", "source": {"base_url": "http://a", "endpoint": "/b"},
                "destination": {"conn_env": "PG_CONN", "table": "t", "primary_key": "id"}}
        api2sql.validate_config(json.loads(json.dumps(good)))
        cases = [
            ({"source": good["source"], "destination": good["destination"]}, "name"),
            ({**good, "source": {"base_url": "http://a"}}, "endpoint"),
            ({**good, "source": {**good["source"], "pagination": {"type": "carrier_pigeon"}}}, "pagination type"),
            ({**good, "source": {**good["source"], "pagination": {"type": "next_cursor"}}}, "cursor_path"),
            ({**good, "source": {**good["source"], "rate_limit": {"requests_per_minute": 0}}}, "requests_per_minute"),
            ({**good, "source": {**good["source"], "incremental": {"cursor_field": "x"}}}, "cursor_param"),
            ({**good, "destination": {**good["destination"], "write_mode": "append"}}, "write_mode"),
            ({**good, "destination": {"conn_env": "PG_CONN", "table": "t"}}, "primary_key"),
        ]
        for bad, needle in cases:
            with self.subTest(needle=needle), self.assertRaises(api2sql.ConfigError) as cm:
                api2sql.validate_config(json.loads(json.dumps(bad)))
            self.assertIn(needle, str(cm.exception))


# ======================================================================= retries and rate limits (no database)

class HttpBehaviourTests(unittest.TestCase):
    def setUp(self):
        self.api = MockAPI(make_records(5), "link")
        self.addCleanup(self.api.stop)
        self.sleeps = []
        self.src = {"base_url": self.api.base_url, "endpoint": "/items"}

    def client(self, **kw):
        return api2sql.Client(self.src, sleep=self.sleeps.append, **kw)

    def get(self, client):
        return quiet(client.get, self.api.base_url + "/items")

    def test_429_and_5xx_retry_with_exponential_backoff(self):
        self.api.failures = [(503, {}), (500, {}), (429, {})]
        self.assertEqual(self.get(self.client(backoff_base=1.0)).status_code, 200)
        self.assertEqual(len(self.sleeps), 3)
        for attempt, waited in enumerate(self.sleeps):      # 1s, 2s, 4s, each plus up to 1s jitter
            self.assertTrue(2 ** attempt <= waited < 2 ** attempt + 1, (attempt, waited))

    def test_retry_after_header_is_respected(self):
        self.api.failures = [(429, {"Retry-After": "3"})]
        self.get(self.client())
        self.assertEqual(self.sleeps, [3.0])

    def test_gives_up_after_5_retries(self):
        self.api.failures = [(503, {})] * 20
        with self.assertRaises(api2sql.ApiError) as cm:
            self.get(self.client())
        self.assertEqual(len(self.api.log), 6)              # the first try + 5 retries
        self.assertEqual(len(self.sleeps), 5)
        self.assertIn("Giving up", str(cm.exception))

    def test_client_errors_do_not_retry(self):
        self.api.failures = [(401, {})]
        with self.assertRaises(api2sql.ApiError) as cm:
            self.get(self.client())
        self.assertEqual(len(self.api.log), 1)
        self.assertIn("token", str(cm.exception))

    def test_connection_errors_retry_too(self):
        client = api2sql.Client({"base_url": "http://127.0.0.1:9", "endpoint": "/"}, sleep=self.sleeps.append)
        with self.assertRaises(api2sql.ApiError):
            quiet(client.get, "http://127.0.0.1:9/")
        self.assertEqual(len(self.sleeps), 5)

    def test_exhausted_rate_limit_with_far_reset_stops_cleanly(self):
        reset = str(int(time.time()) + 3600)
        self.api.failures = [(403, {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": reset})]
        with self.assertRaises(api2sql.RateLimitExhausted):
            self.get(self.client())

    def test_waits_for_reset_when_the_wait_is_short(self):
        self.api.extra_headers = {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": str(int(time.time()) + 5)}
        client = self.client()
        self.get(client)                                    # response says: no requests left
        self.get(client)                                    # so the next call should wait for the reset first
        self.assertEqual(len(self.sleeps), 1)
        self.assertTrue(4 <= self.sleeps[0] <= 7, self.sleeps)


# ======================================================================= integration tests (real Postgres)

@unittest.skipUnless(PG_CONN, "set PG_CONN to run the database tests")
class SyncTests(unittest.TestCase):
    def setUp(self):
        self.conn = psycopg2.connect(PG_CONN)
        self.apis, self.tables = [], []
        self.addCleanup(self.cleanup)

    def cleanup(self):
        self.conn.rollback()
        with self.conn.cursor() as cur:
            for t in self.tables:
                cur.execute(f'DROP TABLE IF EXISTS "{t}"')
            cur.execute("CREATE TABLE IF NOT EXISTS _sync_state (name TEXT PRIMARY KEY, cursor_field TEXT, cursor_value TEXT, last_sync_at TIMESTAMPTZ, last_run_rows INTEGER, last_run_mode TEXT)")
            cur.execute("DELETE FROM _sync_state WHERE name LIKE 't\\_%'")
        self.conn.commit()
        self.conn.close()
        for a in self.apis:
            a.stop()

    def setup_api(self, style="link", records=None):
        api = MockAPI(records or make_records(25), style)
        self.apis.append(api)
        table = f"t_{style}_{len(self.tables)}_{int(time.time() * 1000) % 100000}"
        self.tables.append(table)
        return api, table

    def run_sync(self, cfg, **kw):
        client = api2sql.Client(cfg["source"], sleep=lambda s: None)
        return quiet(api2sql.sync, cfg, self.conn, client=client, **kw)

    def query(self, sql_text, args=None):
        with self.conn.cursor() as cur:
            cur.execute(sql_text, args)
            rows = cur.fetchall()
        self.conn.commit()
        return rows

    def count(self, table):
        return self.query(f'SELECT count(*) FROM "{table}"')[0][0]

    def state(self, name):
        return self.query("SELECT cursor_value FROM _sync_state WHERE name = %s", (name,))[0][0]

    # ---- the five pagination styles
    def test_every_pagination_style_loads_everything(self):
        for style in PAGINATION:
            with self.subTest(style=style):
                api, table = self.setup_api(style)
                result = self.run_sync(make_cfg(api, table, incremental=False))
                self.assertEqual(self.count(table), 25)
                self.assertEqual(result["pages"], 3)
                self.assertEqual(len(api.log), 3)           # 25 records at 10 per page = 3 requests

    # ---- idempotent upsert
    def test_running_twice_does_not_duplicate_and_changes_are_applied(self):
        api, table = self.setup_api()
        cfg = make_cfg(api, table, incremental=False)
        self.run_sync(cfg)
        self.run_sync(cfg)
        self.assertEqual(self.count(table), 25)
        api.records[4]["title"] = "renamed"                 # the API changes a record...
        self.run_sync(cfg)                                  # ...and the next run updates the same row
        self.assertEqual(self.count(table), 25)
        self.assertEqual(self.query(f'SELECT title FROM "{table}" WHERE id = 5')[0][0], "renamed")

    # ---- nested JSON -> flat columns
    def test_nested_json_becomes_flat_columns_and_new_fields_add_columns(self):
        api, table = self.setup_api()
        self.run_sync(make_cfg(api, table, incremental=False))
        cols = dict(self.query("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s", (table,)))
        self.assertEqual(cols["user_login"], "text")
        self.assertEqual(cols["user_id"], "bigint")
        self.assertEqual(cols["labels"], "jsonb")
        self.assertEqual(cols["updated_at"], "timestamp with time zone")
        self.assertIn("milestone_title", cols)              # first appeared on page 2: column was added
        self.assertEqual(self.query(f'SELECT milestone_title FROM "{table}" WHERE id = 20')[0][0], "v2")
        self.assertIsNone(self.query(f'SELECT milestone_title FROM "{table}" WHERE id = 3')[0][0])
        self.assertEqual(self.query(f'SELECT labels->0->>\'name\' FROM "{table}" WHERE id = 1')[0][0], "bug")

    # ---- incremental
    def test_incremental_resumes_from_the_saved_cursor(self):
        api, table = self.setup_api()
        cfg = make_cfg(api, table)
        first = self.run_sync(cfg)
        self.assertEqual(first["mode"], "initial")
        self.assertEqual(self.state(cfg["name"]), iso(25))
        self.assertNotIn("since", api.log[0][1])            # nothing saved yet: fetch everything

        api.log.clear()
        second = self.run_sync(cfg)                         # nothing new
        self.assertEqual(second["mode"], "incremental")
        self.assertEqual(api.log[0][1]["since"], iso(25))
        self.assertLessEqual(second["rows"], 1)             # at most the boundary record again
        self.assertEqual(self.count(table), 25)

        api.records += [{"id": i, "title": f"issue {i}", "updated_at": iso(i), "user": {"login": f"u{i}", "id": i},
                         "labels": [], "assignee": None} for i in (26, 27, 28)]
        api.records[4]["updated_at"], api.records[4]["title"] = iso(40), "edited later"
        api.log.clear()
        third = self.run_sync(cfg)
        self.assertEqual(third["rows"], 5)                  # boundary record, 3 new, 1 edited
        self.assertEqual(self.count(table), 28)
        self.assertEqual(self.query(f'SELECT title FROM "{table}" WHERE id = 5')[0][0], "edited later")
        self.assertEqual(self.state(cfg["name"]), iso(40))

    def test_full_refresh_ignores_the_cursor(self):
        api, table = self.setup_api()
        cfg = make_cfg(api, table)
        self.run_sync(cfg)
        api.log.clear()
        result = self.run_sync(cfg, full_refresh=True)
        self.assertEqual(result["mode"], "full-refresh")
        self.assertNotIn("since", api.log[0][1])
        self.assertEqual(result["rows"], 25)
        self.assertEqual(self.count(table), 25)

    def test_lookback_window_refetches_recent_history(self):
        api, table = self.setup_api()
        cfg = make_cfg(api, table)
        self.run_sync(cfg)
        api.log.clear()
        self.run_sync(cfg, lookback="7d")
        expected = api2sql.format_dt(BASE + timedelta(minutes=25) - timedelta(days=7))
        self.assertEqual(api.log[0][1]["since"], expected)
        self.assertEqual(self.count(table), 25)

    def test_first_run_can_start_from_an_initial_lookback(self):
        api, table = self.setup_api()
        cfg = make_cfg(api, table, initial_lookback="30d")
        self.run_sync(cfg, now=BASE + timedelta(minutes=25))
        self.assertEqual(api.log[0][1]["since"], api2sql.format_dt(BASE + timedelta(minutes=25) - timedelta(days=30)))

    # ---- stopping early and resuming
    def test_ascending_api_can_be_loaded_in_resumable_chunks(self):
        api, table = self.setup_api()
        cfg = make_cfg(api, table, ascending=True)
        r1 = self.run_sync(cfg, max_records=10)
        self.assertEqual(r1["rows"], 10)
        self.assertEqual(self.state(cfg["name"]), iso(10))  # progress saved so the next run continues from here
        for _ in range(3):
            self.run_sync(cfg, max_records=10)
        self.assertEqual(self.count(table), 25)
        self.assertEqual(self.state(cfg["name"]), iso(25))

    def test_cursor_is_not_advanced_when_stopping_early_on_an_unsorted_api(self):
        api, table = self.setup_api()
        cfg = make_cfg(api, table, ascending=False)
        self.run_sync(cfg, max_records=10)
        self.assertEqual(self.count(table), 10)
        self.assertIsNone(self.state(cfg["name"]))

    def test_a_failure_midway_keeps_the_pages_already_saved(self):
        api, table = self.setup_api()
        cfg = make_cfg(api, table, ascending=True)
        client = api2sql.Client(cfg["source"], sleep=lambda s: None)
        original = client.get
        calls = []

        def flaky(url, params=None):
            calls.append(url)
            if len(calls) == 3:
                raise api2sql.ApiError("simulated outage")
            return original(url, params)

        client.get = flaky
        with self.assertRaises(api2sql.ApiError):
            quiet(api2sql.sync, cfg, self.conn, client=client)
        self.assertEqual(self.count(table), 20)             # pages 1 and 2 were committed
        self.assertEqual(self.state(cfg["name"]), iso(20))  # and the cursor points just past them
        self.run_sync(cfg)                                  # the next run picks up from there
        self.assertEqual(self.count(table), 25)

    # ---- status and command line
    def test_status_reports_last_sync_cursor_and_row_count(self):
        api, table = self.setup_api()
        cfg = make_cfg(api, table)
        lines = []
        api2sql.show_status(cfg, self.conn, out=lines.append)
        self.assertIn("never", "\n".join(lines))
        self.run_sync(cfg)
        lines.clear()
        api2sql.show_status(cfg, self.conn, out=lines.append)
        text = "\n".join(lines)
        self.assertIn("Rows in table: 25", text)
        self.assertIn(iso(25), text)
        self.assertIn("initial", text)

    def test_command_line_end_to_end(self):
        api, table = self.setup_api()
        cfg = make_cfg(api, table)
        with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as f:
            yaml.safe_dump(cfg, f)
        self.addCleanup(os.unlink, f.name)
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            self.assertEqual(api2sql.main(["run", f.name]), 0)
            self.assertEqual(api2sql.main(["run", f.name, "--full-refresh"]), 0)
            self.assertEqual(api2sql.main(["status", f.name]), 0)
            self.assertEqual(api2sql.main(["run", "no_such_file.yml"]), 2)
        self.assertIn("Rows in table: 25", out.getvalue())
        self.assertIn("Config file not found", err.getvalue())
        saved = os.environ.pop("PG_CONN")
        try:
            with contextlib.redirect_stderr(io.StringIO()) as e2:
                self.assertEqual(api2sql.main(["status", f.name]), 2)
            self.assertIn("PG_CONN", e2.getvalue())
        finally:
            os.environ["PG_CONN"] = saved


if __name__ == "__main__":
    unittest.main()
