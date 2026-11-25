"""
Tests for schema_diff.py (the stretch goal). They create and drop two throwaway databases,
mm_diff_a_test and mm_diff_b_test, so they need a Postgres user that can CREATE DATABASE:

    export PG_CONN=postgresql://postgres:password@localhost:5432/migrate_test
    python3 -m unittest test_schema_diff -v

The main idea is a ROUND TRIP: generate the SQL that turns A into B, run it on A, and A
must then match B exactly (no differences in either direction). The DOWN SQL must take
A back to what it was.
"""

import contextlib
import io
import os
import tempfile
import unittest
from pathlib import Path

import psycopg2
from psycopg2.extensions import make_dsn, parse_dsn

import mini_migrate as mm
import schema_diff as sd

PG_CONN = os.environ.get("PG_CONN")
SAFE = bool(PG_CONN and "test" in parse_dsn(PG_CONN).get("dbname", ""))
DB_A, DB_B = "mm_diff_a_test", "mm_diff_b_test"

SETUP_A = """
CREATE TABLE customers (id SERIAL PRIMARY KEY, name TEXT NOT NULL, nickname TEXT, legacy TEXT);
CREATE TABLE old_stuff (id INTEGER PRIMARY KEY);
CREATE INDEX customers_name_idx ON customers (name);
"""
SETUP_B = """
CREATE TABLE customers (id SERIAL PRIMARY KEY, name TEXT DEFAULT 'anon', nickname VARCHAR(30),
                        email TEXT, phone VARCHAR(20) NOT NULL DEFAULT '');
CREATE UNIQUE INDEX customers_email_idx ON customers (email);
CREATE TABLE orders (
    id BIGSERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers (id),
    amount NUMERIC(12, 2) NOT NULL CHECK (amount >= 0),
    status TEXT DEFAULT 'new',
    UNIQUE (customer_id, status)
);
CREATE INDEX orders_status_idx ON orders (status);
"""


def dsn(dbname):
    return make_dsn(PG_CONN, dbname=dbname)


def recreate(dbname):
    admin = psycopg2.connect(PG_CONN)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid()", (dbname,))
        cur.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
        cur.execute(f'CREATE DATABASE "{dbname}"')
    admin.close()


def drop(dbname):
    admin = psycopg2.connect(PG_CONN)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid()", (dbname,))
        cur.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
    admin.close()


def run_sql(dbname, sql):
    conn = psycopg2.connect(dsn(dbname))
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


def schema_of(dbname):
    conn = psycopg2.connect(dsn(dbname))
    try:
        return sd.introspect(conn)
    finally:
        conn.close()


@unittest.skipUnless(SAFE, 'set PG_CONN to a database whose name contains "test"')
class SchemaDiffTests(unittest.TestCase):
    def setUp(self):
        recreate(DB_A)
        recreate(DB_B)
        self.addCleanup(drop, DB_A)
        self.addCleanup(drop, DB_B)

    def assertSameSchema(self, db_x, db_y, msg=None):
        x, y = schema_of(db_x), schema_of(db_y)
        self.assertEqual(sd.diff(x, y), [], msg)
        self.assertEqual(sd.diff(y, x), [], msg)

    # ---- nothing to do
    def test_identical_schemas_produce_no_sql(self):
        run_sql(DB_A, SETUP_B)
        run_sql(DB_B, SETUP_B)
        self.assertEqual(sd.diff(schema_of(DB_A), schema_of(DB_B)), [])

    def test_the_tools_own_table_is_ignored(self):
        run_sql(DB_A, SETUP_B + "CREATE TABLE schema_migrations (version TEXT PRIMARY KEY);")
        run_sql(DB_B, SETUP_B)
        self.assertEqual(sd.diff(schema_of(DB_A), schema_of(DB_B)), [])

    # ---- the round trip
    def test_up_turns_a_into_b_and_down_turns_it_back(self):
        run_sql(DB_A, SETUP_A)
        run_sql(DB_B, SETUP_B)
        a_before, b = schema_of(DB_A), schema_of(DB_B)
        up, down = sd.render_sql(sd.diff(a_before, b)), sd.render_sql(sd.diff(b, a_before))

        for needle in ('CREATE TABLE "orders"', "BIGSERIAL", 'ADD COLUMN "email"', 'DROP COLUMN "legacy"',
                       'DROP TABLE "old_stuff"', "TYPE character varying(30)", "DROP NOT NULL", "SET DEFAULT 'anon'",
                       'ADD COLUMN "phone"', "customers_email_idx", 'DROP INDEX IF EXISTS "customers_name_idx"'):
            self.assertIn(needle, up)
        self.assertNotIn("nextval", up)                                    # SERIAL, not a reference to a missing sequence
        self.assertLess(up.index("Create new tables"), up.index("DESTRUCTIVE"))     # drops come last

        run_sql(DB_A, up)
        self.assertSameSchema(DB_A, DB_B, "after UP, A should match B")
        run_sql(DB_A, down)
        a_after = schema_of(DB_A)
        self.assertEqual(sd.diff(a_after, a_before), [], "after DOWN, A should be what it was")
        self.assertEqual(sd.diff(a_before, a_after), [])

    def test_changed_constraints_indexes_and_primary_keys_are_replaced(self):
        run_sql(DB_A, """CREATE TABLE t (id INTEGER, qty INTEGER, CONSTRAINT t_pkey PRIMARY KEY (id),
                         CONSTRAINT qty_check CHECK (qty > 0)); CREATE INDEX t_qty_idx ON t (qty);""")
        run_sql(DB_B, """CREATE TABLE t (id INTEGER, qty INTEGER, region TEXT NOT NULL,
                         CONSTRAINT t_pkey PRIMARY KEY (id, region), CONSTRAINT qty_check CHECK (qty >= 0));
                         CREATE INDEX t_qty_idx ON t (qty, id);""")
        up = sd.render_sql(sd.diff(schema_of(DB_A), schema_of(DB_B)))
        for needle in ('DROP CONSTRAINT IF EXISTS "qty_check"', 'ADD CONSTRAINT "qty_check"', 'DROP CONSTRAINT IF EXISTS "t_pkey"',
                       'DROP INDEX IF EXISTS "t_qty_idx"'):
            self.assertIn(needle, up)
        run_sql(DB_A, up)
        self.assertSameSchema(DB_A, DB_B)

    def test_foreign_keys_do_not_block_dropping_tables(self):
        run_sql(DB_A, "CREATE TABLE parent (id SERIAL PRIMARY KEY); CREATE TABLE child (id SERIAL PRIMARY KEY, "
                      "parent_id INTEGER REFERENCES parent (id));")
        run_sql(DB_B, "CREATE TABLE unrelated (id SERIAL PRIMARY KEY);")
        up = sd.render_sql(sd.diff(schema_of(DB_A), schema_of(DB_B)))
        self.assertLess(up.index("DROP CONSTRAINT"), up.index("DROP TABLE"))
        run_sql(DB_A, up)
        self.assertSameSchema(DB_A, DB_B)

    def test_unusual_names_identity_and_generated_columns(self):
        run_sql(DB_A, "CREATE TABLE keep (id INTEGER PRIMARY KEY);")
        run_sql(DB_B, """CREATE TABLE keep (id INTEGER PRIMARY KEY);
                         CREATE TABLE "Order Items" (
                             "Item Id" INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                             "Unit Price" NUMERIC(10, 2) NOT NULL DEFAULT 0,
                             "select" TEXT,
                             doubled NUMERIC GENERATED ALWAYS AS ("Unit Price" * 2) STORED);""")
        up = sd.render_sql(sd.diff(schema_of(DB_A), schema_of(DB_B)))
        self.assertIn('"Order Items"', up)
        self.assertIn("GENERATED BY DEFAULT AS IDENTITY", up)
        run_sql(DB_A, up)
        self.assertSameSchema(DB_A, DB_B)

    def test_a_not_null_column_without_a_default_gets_a_warning(self):
        run_sql(DB_A, "CREATE TABLE t (id INTEGER PRIMARY KEY);")
        run_sql(DB_B, "CREATE TABLE t (id INTEGER PRIMARY KEY, must_have TEXT NOT NULL);")
        self.assertIn("-- NOTE: t.must_have is NOT NULL", sd.render_sql(sd.diff(schema_of(DB_A), schema_of(DB_B))))

    # ---- through the command line
    def test_the_diff_command_prints_sql_or_says_nothing_differs(self):
        run_sql(DB_A, SETUP_A)
        run_sql(DB_B, SETUP_B)
        lines = []
        self.assertEqual(mm.main(["diff", dsn(DB_A), dsn(DB_B)], out=lines.append), 0)
        self.assertIn("SQL that turns the LOCAL schema into the TARGET schema", "\n".join(lines))
        self.assertIn('CREATE TABLE "orders"', "\n".join(lines))
        run_sql(DB_A, "\n".join(lines))
        lines.clear()
        mm.main(["diff", dsn(DB_A), dsn(DB_B)], out=lines.append)
        self.assertIn("No differences", "\n".join(lines))

    def test_diff_write_creates_a_migration_that_up_and_down_can_run(self):
        folder = tempfile.mkdtemp()
        self.addCleanup(lambda: __import__("shutil").rmtree(folder, ignore_errors=True))
        run_sql(DB_A, SETUP_A)
        run_sql(DB_B, SETUP_B)
        a_before = schema_of(DB_A)
        out = []
        self.assertEqual(mm.main(["diff", dsn(DB_A), dsn(DB_B), "--write", "sync with b", "--migrations-dir", folder],
                                 out=out.append), 0)
        path = Path(folder, "V001__sync_with_b.sql")
        self.assertTrue(path.exists())
        migration = mm.parse_migration(path.name, path.read_text())       # a normal, valid migration file
        self.assertIn('CREATE TABLE "orders"', migration.up_sql)
        self.assertTrue(migration.reversible)

        quiet = lambda s: None
        base = ["--dsn", dsn(DB_A), "--migrations-dir", folder]
        self.assertEqual(mm.main([*base, "init"], out=quiet), 0)
        self.assertEqual(mm.main([*base, "up"], out=quiet), 0)
        self.assertSameSchema(DB_A, DB_B, "after `up`, the local database should match the target")
        self.assertEqual(mm.main([*base, "down"], out=quiet), 0)
        self.assertEqual(sd.diff(schema_of(DB_A), a_before), [], "after `down`, the schema should be back as it was")


if __name__ == "__main__":
    unittest.main()
