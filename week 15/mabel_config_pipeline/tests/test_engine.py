"""End-to-end tests. A local HTTP server stands in for the real APIs."""
import json
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd
import pytest

from config_pipeline.base import ConfigError
from config_pipeline.cli import main
from config_pipeline.engine import run_file, validate_file


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlparse(self.path)
        q = parse_qs(url.query)
        if url.path == "/coins/markets":  # list-of-records JSON, paginated (2 pages of data)
            page = int(q.get("page", ["1"])[0])
            rows = [] if page > 2 else [
                {"id": f"coin{page}{i}", "symbol": "x", "name": "X", "current_price": 1.5 * i,
                 "market_cap": 100, "total_volume": 10, "roi": {"times": 2}} for i in range(3)
            ]
            body = rows
        elif url.path == "/forecast":  # columnar JSON nested under "hourly"
            body = {"hourly": {"time": ["2026-01-01T00:00", "2026-01-01T01:00", "2026-01-01T02:00"],
                               "temperature_2m": [24.1, 23.8, None]}}
        else:
            self.send_response(404); self.end_headers(); return
        data = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *a):
        pass


@pytest.fixture(scope="module")
def server():
    srv = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_port}"
    srv.shutdown()


def write(tmp_path, name, text):
    p = tmp_path / name
    p.write_text(text)
    return str(p)


def crypto_yaml(base, db):
    return f"""
name: crypto
source:
  type: rest_api
  config:
    base_url: {base}
    endpoint: /coins/markets
    pagination: {{type: page, param_name: page, max_pages: 5}}
transforms:
  - type: select_columns
    columns: [id, symbol, name, current_price]
  - type: add_column
    name: ingested_at
    value: "{{{{ now() }}}}"
destination:
  type: sqlite
  config: {{path: {db}, table: coin_prices, primary_key: [id, ingested_at], on_conflict: ignore}}
"""


def test_crypto_pipeline_paginates_and_stops_on_empty_page(server, tmp_path):
    db = tmp_path / "c.db"
    stats = run_file(write(tmp_path, "crypto.yml", crypto_yaml(server, db)))
    assert stats["rows_read"] == 6  # 2 pages x 3 rows; page 3 was empty so it stopped
    with sqlite3.connect(db) as conn:
        assert conn.execute("select count(*) from coin_prices").fetchone()[0] == 6


def test_weather_pipeline_columnar_json_to_parquet(server, tmp_path):
    out = tmp_path / "w.parquet"
    yml = f"""
name: weather
source:
  type: rest_api
  config: {{base_url: {server}, endpoint: /forecast, records_path: hourly}}
transforms:
  - {{type: rename_column, mapping: {{time: observed_at}}}}
  - {{type: filter_rows, column: temperature_2m, op: not_null}}
  - {{type: add_column, name: city, value: Lagos}}
destination:
  type: parquet_file
  config: {{path: {out}, mode: append, primary_key: [city, observed_at]}}
"""
    p = write(tmp_path, "weather.yml", yml)
    run_file(p)
    run_file(p)  # run twice: primary_key dedupe means no duplicate rows
    df = pd.read_parquet(out)
    assert len(df) == 2 and set(df.columns) == {"observed_at", "temperature_2m", "city"}


def test_csv_ingest_is_idempotent_with_replace(tmp_path):
    csv = write(tmp_path, "in.csv", "id,amount\n1,10\n2,-5\n3,30\n")
    db = tmp_path / "o.db"
    yml = f"""
name: csv
source: {{type: csv_file, config: {{path: {csv}}}}}
transforms:
  - {{type: filter_rows, column: amount, op: gt, value: 0}}
destination:
  type: sqlite
  config: {{path: {db}, table: t, primary_key: [id], on_conflict: replace}}
"""
    p = write(tmp_path, "csv.yml", yml)
    run_file(p); run_file(p)
    with sqlite3.connect(db) as conn:
        assert conn.execute("select id from t order by id").fetchall() == [(1,), (3,)]


def test_validate_reports_all_problems_with_hints(tmp_path):
    p = write(tmp_path, "bad.yml", """
name: bad
source:
  type: rest_apii
destination:
  type: sqlite
  config: {path: x.db, tabel: t}
transforms:
  - type: cast_column
    column: a
    dtype: integer
""")
    with pytest.raises(ConfigError) as exc:
        validate_file(p)
    text = str(exc.value)
    assert "did you mean 'rest_api'" in text
    assert "did you mean 'table'" in text
    assert "destination.config.table: required" in text
    assert "dtype: must be one of" in text


def test_validate_does_not_run_anything(tmp_path):
    db = tmp_path / "never.db"
    validate_file(write(tmp_path, "ok.yml", crypto_yaml("http://127.0.0.1:1", db)))  # unreachable URL
    assert not db.exists()


def test_bad_yaml_syntax_gives_line_number(tmp_path):
    p = write(tmp_path, "broken.yml", "name: x\nsource: [unclosed\n")
    with pytest.raises(ConfigError, match="line"):
        validate_file(p)


def test_missing_env_var_fails_at_run_but_not_validate(tmp_path, monkeypatch):
    monkeypatch.delenv("NOPE_PATH", raising=False)
    p = write(tmp_path, "env.yml", """
name: e
source: {type: csv_file, config: {path: "{{ env.NOPE_PATH }}"}}
destination: {type: sqlite, config: {path: o.db, table: t}}
""")
    validate_file(p)
    with pytest.raises(ConfigError, match="NOPE_PATH"):
        run_file(p)


def test_cli_exit_codes(tmp_path, capsys):
    good = write(tmp_path, "g.yml", "name: g\nsource: {type: csv_file, config: {path: a.csv}}\n"
                 "destination: {type: sqlite, config: {path: o.db, table: t}}\n")
    bad = write(tmp_path, "b.yml", "name: b\n")
    assert main(["validate", good]) == 0
    assert main(["validate", bad]) == 2
    assert main(["run", good]) == 1  # valid config, but a.csv does not exist
    assert "not found" in capsys.readouterr().err


def test_engine_has_no_api_specific_code():
    src = "".join(p.read_text().lower() for p in Path("config_pipeline").glob("*.py"))
    assert "coingecko" not in src and "open-meteo" not in src and "weather" not in src
