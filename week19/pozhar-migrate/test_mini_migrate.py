"""
Tests for mini_migrate.py. The database tests need a THROWAWAY Postgres database:

    sudo docker exec -it pg-cdc psql -U postgres -c "CREATE DATABASE migrate_test;"
    export PG_CONN=postgresql://postgres:password@localhost:5432/migrate_test
    python3 -m unittest test_mini_migrate -v

WARNING: they DROP everything in the public schema of that database before each test,
so they refuse to run unless the database name contains "test".
"""

import contextlib
import io
import os
import tempfile
import unittest
from pathlib import Path

import psycopg2
from psycopg2.extensions import parse_dsn

import mini_migrate as mm

PG_CONN = os.environ.get("PG_CONN")
SAFE = bool(PG_CONN and "test" in parse_dsn(PG_CONN).get("dbname", ""))
NEEDS_DB = unittest.skipUnless(SAFE, 'set PG_CONN to a database whose name contains "test"')

V1 = "-- UP\nCREATE TABLE customers (id SERIAL PRIMARY KEY, name TEXT NOT NULL);\n\n-- DOWN\nDROP TABLE customers;\n"
V2 = "-- UP\nALTER TABLE customers ADD COLUMN email TEXT;\nCREATE INDEX customers_email_idx ON customers (email);\n\n-- DOWN\nDROP INDEX customers_email_idx;\nALTER TABLE customers DROP COLUMN email;\n"
V3 = "-- UP\nALTER TABLE customers ADD COLUMN phone TEXT;\n\n-- DOWN\nALTER TABLE customers DROP COLUMN phone;\n"
BASIC = {"V001__create_customers.sql": V1, "V002__add_email_index.sql": V2, "V003__add_phone_column.sql": V3}


def migration(number, description="thing", up="SELECT 1;", down="SELECT 2;", text=None):
    text = text or f"-- UP\n{up}\n\n-- DOWN\n{down}\n"
    return mm.parse_migration(f"V{number:03d}__{description}.sql", text)


# ======================================================================= no database needed

class ParsingTests(unittest.TestCase):
    def test_a_valid_file_is_split_into_up_and_down(self):
        m = mm.parse_migration("V001__create_customers.sql", V1)
        self.assertEqual((m.number, m.version, m.description), (1, "V001", "create customers"))
        self.assertIn("CREATE TABLE customers", m.up_sql)
        self.assertEqual(m.down_sql, "DROP TABLE customers;")
        self.assertTrue(m.reversible)

    def test_markers_are_forgiving_and_text_before_up_is_ignored(self):
        m = mm.parse_migration("V002__x.sql", "-- Notes about this change\n--   up  \nSELECT 1;\n-- Down\nSELECT 2;\n")
        self.assertEqual((m.up_sql, m.down_sql), ("SELECT 1;", "SELECT 2;"))

    def test_bad_files_are_rejected_with_a_clear_message(self):
        cases = [
            ("V001__x.sql", "-- DOWN\nSELECT 1;\n", "no '-- UP' section"),
            ("V001__x.sql", "-- UP\nSELECT 1;\n", "no '-- DOWN' section"),
            ("V001__x.sql", "-- UP\n-- just a comment\n-- DOWN\nSELECT 1;\n", "empty '-- UP'"),
            ("V001__x.sql", "-- UP\nSELECT 1;\n-- UP\nSELECT 2;\n-- DOWN\nSELECT 3;\n", "two '-- UP'"),
            ("001_x.sql", V1, "doesn't match"),
            ("V1-create.sql", V1, "doesn't match"),
        ]
        for name, text, needle in cases:
            with self.subTest(name=name, needle=needle), self.assertRaises(mm.MigrationError) as cm:
                mm.parse_migration(name, text)
            self.assertIn(needle, str(cm.exception))

    def test_an_empty_down_section_means_irreversible(self):
        m = mm.parse_migration("V001__x.sql", "-- UP\nSELECT 1;\n-- DOWN\n-- cannot be undone\n")
        self.assertFalse(m.reversible)

    def test_migrations_may_not_control_transactions_themselves(self):
        for bad in ("BEGIN;", "COMMIT;", "rollback;", "START TRANSACTION;", "BEGIN TRANSACTION;", "COMMIT"):
            with self.subTest(bad=bad), self.assertRaises(mm.MigrationError) as cm:
                mm.parse_migration("V001__x.sql", f"-- UP\n{bad}\nSELECT 1;\n-- DOWN\nSELECT 2;\n")
            self.assertIn("BEGIN/COMMIT/ROLLBACK", str(cm.exception))

    def test_transaction_words_inside_function_bodies_strings_and_comments_are_fine(self):
        text = ("-- UP\nCREATE FUNCTION f() RETURNS int AS $$\nBEGIN\n  RETURN 1;\nEND;\n$$ LANGUAGE plpgsql;\n"
                "INSERT INTO t VALUES ('COMMIT;');\n-- COMMIT;\n-- DOWN\nDROP FUNCTION f();\n")
        self.assertTrue(mm.parse_migration("V001__x.sql", text).up_sql)

    def test_checksum_ignores_line_endings_but_not_edits(self):
        self.assertEqual(mm.checksum(V1), mm.checksum(V1.replace("\n", "\r\n")))
        self.assertNotEqual(mm.checksum(V1), mm.checksum(V1 + "-- one more comment\n"))
        self.assertEqual(len(mm.checksum(V1)), 64)

    def test_versions_can_be_written_several_ways(self):
        for text in ("V002", "v002", "2", "002", " V2 "):
            self.assertEqual(mm.parse_version(text), 2)
        for bad in ("two", "V", "V2x", ""):
            with self.assertRaises(mm.MigrationError):
                mm.parse_version(bad)


class FolderAndNewTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(lambda: __import__("shutil").rmtree(self.dir, ignore_errors=True))

    def write(self, files):
        for name, text in files.items():
            Path(self.dir, name).write_text(text)

    def test_files_are_loaded_in_numeric_order(self):
        self.write({"V010__ten.sql": V1, "V002__two.sql": V1, "V001__one.sql": V1, "notes.txt": "ignored"})
        self.assertEqual([m.version for m in mm.load_migrations(self.dir)], ["V001", "V002", "V010"])

    def test_duplicate_numbers_and_wrongly_named_files_are_refused(self):
        self.write({"V001__a.sql": V1, "V001__b.sql": V1})
        with self.assertRaises(mm.MigrationError) as cm:
            mm.load_migrations(self.dir)
        self.assertIn("both use version V001", str(cm.exception))
        Path(self.dir, "V001__b.sql").unlink()
        self.write({"create_orders.sql": V1})
        with self.assertRaises(mm.MigrationError):
            mm.load_migrations(self.dir)

    def test_a_missing_folder_is_a_config_error(self):
        with self.assertRaises(mm.ConfigError):
            mm.load_migrations(os.path.join(self.dir, "nope"))

    def test_new_creates_the_next_numbered_file(self):
        out = []
        self.assertEqual(mm.cmd_new(self.dir, "create customers", out.append), 0)
        self.assertEqual(sorted(os.listdir(self.dir)), ["V001__create_customers.sql"])
        self.write({"V007__seven.sql": V1})
        mm.cmd_new(self.dir, "Add Phone Column!!", out.append)
        self.assertIn("V008__add_phone_column.sql", os.listdir(self.dir))   # next after the highest, even with gaps
        self.assertIn("V008__add_phone_column.sql", "\n".join(out))

    def test_new_makes_the_folder_and_never_overwrites(self):
        folder = os.path.join(self.dir, "fresh", "migrations")
        mm.cmd_new(folder, "first one", lambda s: None)
        self.assertTrue(os.path.exists(os.path.join(folder, "V001__first_one.sql")))
        with self.assertRaises(mm.MigrationError):
            mm.cmd_new(folder, "   ", lambda s: None)

    def test_a_brand_new_migration_must_be_filled_in_before_it_can_run(self):
        mm.cmd_new(self.dir, "write me", lambda s: None)
        with self.assertRaises(mm.MigrationError) as cm:
            mm.load_migrations(self.dir)
        self.assertIn("empty '-- UP'", str(cm.exception))


class AnalysisTests(unittest.TestCase):
    def applied(self, *migrations, **overrides):
        return {m.version: {"description": m.description, "applied_at": None,
                            "checksum": overrides.get(m.version, m.checksum)} for m in migrations}

    def test_pending_and_applied(self):
        a, b, c = migration(1), migration(2), migration(3)
        analysis = mm.analyze([a, b, c], self.applied(a))
        self.assertEqual([(e.version, e.state) for e in analysis.entries],
                         [("V001", "applied"), ("V002", "pending"), ("V003", "pending")])
        self.assertEqual(analysis.problems, [])

    def test_out_of_order_is_detected_and_explained(self):
        a, b, c = migration(1), migration(2), migration(3)
        analysis = mm.analyze([a, b, c], self.applied(a, c))
        self.assertEqual({e.version: e.state for e in analysis.entries}["V002"], "OUT-OF-ORDER")
        text = analysis.problems[0]
        for needle in ("V002", "V003", "version order", "down --to V001"):
            self.assertIn(needle, text)

    def test_a_changed_file_is_detected(self):
        a = migration(1)
        analysis = mm.analyze([a], self.applied(a, V001="0" * 64))
        self.assertEqual(analysis.entries[0].state, "CHANGED")
        self.assertIn("Never edit an applied migration", analysis.problems[0])

    def test_a_missing_file_is_detected(self):
        a, b = migration(1), migration(2)
        analysis = mm.analyze([a], self.applied(a, b))
        self.assertEqual({e.version: e.state for e in analysis.entries}["V002"], "MISSING")

    def test_plans(self):
        a, b, c = migration(1), migration(2), migration(3)
        everything = [a, b, c]
        self.assertEqual([m.version for m in mm.plan_up(everything, {})], ["V001", "V002", "V003"])
        self.assertEqual([m.version for m in mm.plan_up(everything, {}, to="V002")], ["V001", "V002"])
        self.assertEqual([m.version for m in mm.plan_up(everything, self.applied(a), to="2")], ["V002"])
        self.assertEqual(mm.plan_up(everything, self.applied(a, b, c)), [])
        with self.assertRaises(mm.MigrationError):
            mm.plan_up(everything, {}, to="V009")
        applied = self.applied(a, b, c)
        self.assertEqual([m.version for m in mm.plan_down(everything, applied)], ["V003"])
        self.assertEqual([m.version for m in mm.plan_down(everything, applied, to="V001")], ["V003", "V002"])
        self.assertEqual(mm.plan_down(everything, applied, to="V003"), [])
        self.assertEqual(mm.plan_down(everything, {}), [])
        with self.assertRaises(mm.MigrationError):
            mm.plan_down(everything, self.applied(a), to="V002")

    def test_down_only_checks_what_it_is_about_to_undo(self):
        a, b = migration(1), migration(2)
        v1_edited = self.applied(a, b, V001="0" * 64)           # V001 was edited after it was applied...
        self.assertEqual([m.version for m in mm.plan_down([a, b], v1_edited)], ["V002"])   # ...V002 is still safe to undo
        v2_edited = self.applied(a, b, V002="0" * 64)           # but if the one being undone was edited, refuse
        with self.assertRaises(mm.MigrationError) as cm:
            mm.plan_down([a, b], v2_edited)
        self.assertIn("edited", str(cm.exception))
        with self.assertRaises(mm.MigrationError):              # and a file that is gone can't be rolled back
            mm.plan_down([a], self.applied(a, b))


# ======================================================================= database tests

@NEEDS_DB
class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.conn = psycopg2.connect(PG_CONN)
        self.conn.autocommit = True
        with self.conn.cursor() as cur:
            cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        self.conn.autocommit = False
        self.addCleanup(self.conn.close)
        self.dir = tempfile.mkdtemp()
        self.addCleanup(lambda: __import__("shutil").rmtree(self.dir, ignore_errors=True))
        self.write(BASIC)

    def write(self, files):
        for name, text in files.items():
            Path(self.dir, name).write_text(text)

    def run_cli(self, *args, init=True):
        """Runs the real command line. Returns (exit code, stdout, stderr)."""
        out, err = [], io.StringIO()
        with contextlib.redirect_stderr(err):
            code = mm.main(["--dsn", PG_CONN, "--migrations-dir", self.dir, *args], out=out.append)
        return code, "\n".join(out), err.getvalue()

    def query(self, sql_text, args=None):
        with self.conn.cursor() as cur:
            cur.execute(sql_text, args)
            rows = cur.fetchall()
        self.conn.commit()
        return rows

    def versions(self):
        return [r[0] for r in self.query("SELECT version FROM schema_migrations ORDER BY version")]

    def exists(self, name):
        return self.query("SELECT to_regclass(%s) IS NOT NULL", (name,))[0][0]

    def columns(self, table):
        return [r[0] for r in self.query("SELECT column_name FROM information_schema.columns WHERE table_name = %s "
                                         "ORDER BY ordinal_position", (table,))]

    # ---- init and the table itself
    def test_init_creates_the_exact_table_and_is_safe_to_repeat(self):
        code, out, _ = self.run_cli("init")
        self.assertEqual(code, 0)
        self.assertIn("Created the schema_migrations", out)
        cols = self.query("SELECT column_name, data_type, is_nullable FROM information_schema.columns "
                          "WHERE table_name = 'schema_migrations' ORDER BY ordinal_position")
        self.assertEqual(cols, [("version", "text", "NO"), ("description", "text", "YES"),
                                ("applied_at", "timestamp with time zone", "NO"), ("checksum", "text", "NO")])
        pk = self.query("SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
                        "WHERE i.indrelid = 'schema_migrations'::regclass AND i.indisprimary")
        self.assertEqual(pk, [("version",)])
        self.assertIn("already exists", self.run_cli("init")[1])

    def test_everything_but_init_says_so_when_there_is_no_table_yet(self):
        for command in (["status"], ["up"], ["down"]):
            code, _, err = self.run_cli(*command)
            self.assertEqual(code, 1)
            self.assertIn("mini_migrate init", err)

    # ---- up
    def test_up_applies_everything_in_order_and_records_it(self):
        self.run_cli("init")
        code, out, _ = self.run_cli("up")
        self.assertEqual(code, 0)
        self.assertLess(out.index("V001"), out.index("V002"))
        self.assertLess(out.index("V002"), out.index("V003"))
        self.assertEqual(self.columns("customers"), ["id", "name", "email", "phone"])
        rows = self.query("SELECT version, description, checksum, applied_at IS NOT NULL FROM schema_migrations ORDER BY 1")
        self.assertEqual([r[0] for r in rows], ["V001", "V002", "V003"])
        self.assertEqual(rows[1][1], "add email index")
        self.assertEqual(rows[0][2], mm.checksum(V1))
        self.assertTrue(all(r[3] for r in rows))
        self.assertIn("Nothing to apply", self.run_cli("up")[1])            # and it is safe to run again

    def test_up_to_a_version_stops_there(self):
        self.run_cli("init")
        self.run_cli("up", "--to", "V002")
        self.assertEqual(self.versions(), ["V001", "V002"])
        self.assertEqual(self.columns("customers"), ["id", "name", "email"])
        self.assertIn("Nothing to apply", self.run_cli("up", "--to", "V001")[1])
        self.run_cli("up")
        self.assertEqual(self.versions(), ["V001", "V002", "V003"])
        code, _, err = self.run_cli("up", "--to", "V009")
        self.assertEqual(code, 1)
        self.assertIn("no migration file for V009", err)

    # ---- down
    def test_down_rolls_back_one_migration_at_a_time(self):
        self.run_cli("init")
        self.run_cli("up")
        code, out, _ = self.run_cli("down")
        self.assertEqual(code, 0)
        self.assertIn("V003", out)
        self.assertEqual(self.versions(), ["V001", "V002"])
        self.assertEqual(self.columns("customers"), ["id", "name", "email"])
        self.run_cli("down")
        self.run_cli("down")
        self.assertEqual(self.versions(), [])
        self.assertFalse(self.exists("customers"))
        self.assertIn("Nothing to roll back", self.run_cli("down")[1])
        self.run_cli("up")                                                   # and it can all be applied again
        self.assertEqual(self.versions(), ["V001", "V002", "V003"])

    def test_down_to_a_version_rolls_back_everything_newer(self):
        self.run_cli("init")
        self.run_cli("up")
        code, out, _ = self.run_cli("down", "--to", "V001")
        self.assertEqual(code, 0)
        self.assertLess(out.index("V003"), out.index("V002"))               # newest first
        self.assertEqual(self.versions(), ["V001"])
        self.assertEqual(self.columns("customers"), ["id", "name"])
        self.assertIn("Already at V001", self.run_cli("down", "--to", "V001")[1])
        code, _, err = self.run_cli("down", "--to", "V003")                  # V003 is not applied
        self.assertEqual(code, 1)
        self.assertIn("isn't applied", err)

    # ---- safety rule 1: strict order
    def test_out_of_order_migrations_are_refused(self):
        self.run_cli("init")
        self.run_cli("up", "--to", "V001")
        v2 = Path(self.dir, "V002__add_email_index.sql")
        v2.rename(Path(self.dir, "held_back.tmp"))                           # V002 is skipped...
        self.assertEqual(self.run_cli("up")[0], 0)                           # ...so V003 gets applied without it
        self.assertEqual(self.versions(), ["V001", "V003"])
        Path(self.dir, "held_back.tmp").rename(v2)                           # V002 turns up late

        code, _, err = self.run_cli("up")                                    # DoD: skip V002, try to apply -> error
        self.assertEqual(code, 1)
        for needle in ("Refusing to apply", "V002", "V003", "version order"):
            self.assertIn(needle, err)
        self.assertEqual(self.versions(), ["V001", "V003"])                  # and nothing was changed

        code, out, _ = self.run_cli("status")
        self.assertEqual(code, 1)
        self.assertIn("OUT-OF-ORDER", out)

        self.assertEqual(self.run_cli("down", "--to", "V001")[0], 0)         # the documented way out
        self.assertEqual(self.run_cli("up")[0], 0)
        self.assertEqual(self.versions(), ["V001", "V002", "V003"])
        self.assertEqual(self.run_cli("status")[0], 0)

    # ---- safety rule 2: checksums
    def test_editing_an_applied_migration_is_detected(self):
        self.run_cli("init")
        self.run_cli("up", "--to", "V001")
        path = Path(self.dir, "V001__create_customers.sql")
        path.write_text(V1 + "-- a harmless-looking comment\n")

        code, _, err = self.run_cli("up")
        self.assertEqual(code, 1)
        for needle in ("Refusing", "V001", "edited after it was applied", "Never edit"):
            self.assertIn(needle, err)
        code, out, _ = self.run_cli("status")
        self.assertEqual(code, 1)
        self.assertIn("CHANGED", out)
        code, _, err = self.run_cli("down")                                   # rolling back an edited migration is refused too
        self.assertEqual(code, 1)
        self.assertIn("edited", err)
        self.assertEqual(self.versions(), ["V001"])

        path.write_text(V1)                                                   # restore the original: all is well again
        self.assertEqual(self.run_cli("up")[0], 0)
        self.assertEqual(self.versions(), ["V001", "V002", "V003"])

    def test_windows_line_endings_do_not_look_like_an_edit(self):
        self.run_cli("init")
        self.run_cli("up")
        for name, text in BASIC.items():
            Path(self.dir, name).write_bytes(text.replace("\n", "\r\n").encode())
        self.assertEqual(self.run_cli("status")[0], 0)

    def test_a_deleted_file_is_detected(self):
        self.run_cli("init")
        self.run_cli("up")
        Path(self.dir, "V002__add_email_index.sql").unlink()
        code, out, err = self.run_cli("up")
        self.assertEqual(code, 1)
        self.assertIn("no file for it", err)
        self.assertIn("MISSING", self.run_cli("status")[1])

    # ---- safety rule 3: each migration is one transaction
    def test_a_failing_migration_leaves_the_database_unchanged(self):
        self.write({"V004__breaks_half_way.sql":
                    "-- UP\nCREATE TABLE half_done (id INT);\nINSERT INTO half_done VALUES (1);\nSELECT 1/0;\n"
                    "-- DOWN\nDROP TABLE half_done;\n",
                    "V005__never_reached.sql": "-- UP\nCREATE TABLE never_reached (id INT);\n-- DOWN\nDROP TABLE never_reached;\n"})
        self.run_cli("init")
        code, out, err = self.run_cli("up")
        self.assertEqual(code, 1)
        for needle in ("V004", "rolled back", "division by zero", "earlier in this run stay applied"):
            self.assertIn(needle, err)
        self.assertEqual(self.versions(), ["V001", "V002", "V003"])          # earlier ones stay applied
        self.assertFalse(self.exists("half_done"))                            # the CREATE TABLE was undone too
        self.assertFalse(self.exists("never_reached"))                        # and the next one was never attempted
        Path(self.dir, "V004__breaks_half_way.sql").write_text(                # fix the file (it was never applied)
            "-- UP\nCREATE TABLE half_done (id INT);\n-- DOWN\nDROP TABLE half_done;\n")
        self.assertEqual(self.run_cli("up")[0], 0)
        self.assertEqual(self.versions(), ["V001", "V002", "V003", "V004", "V005"])

    def test_a_failing_rollback_changes_nothing_either(self):
        self.write({"V004__x.sql": "-- UP\nCREATE TABLE x (id INT);\n-- DOWN\nDROP TABLE x;\nSELECT 1/0;\n"})
        self.run_cli("init")
        self.run_cli("up")
        code, _, err = self.run_cli("down")
        self.assertEqual(code, 1)
        self.assertIn("rolled back", err)
        self.assertTrue(self.exists("x"))                                     # the DROP was undone
        self.assertIn("V004", self.versions())

    def test_a_migration_with_its_own_commit_is_refused_before_anything_runs(self):
        self.write({"V004__sneaky.sql": "-- UP\nCREATE TABLE sneaky (id INT);\nCOMMIT;\n-- DOWN\nDROP TABLE sneaky;\n"})
        self.run_cli("init")
        code, _, err = self.run_cli("up")
        self.assertEqual(code, 1)
        self.assertIn("BEGIN/COMMIT/ROLLBACK", err)
        self.assertEqual(self.versions(), [])                                 # not even V001 ran

    # ---- rollback safety
    def test_an_irreversible_migration_blocks_a_rollback_before_it_starts(self):
        self.write({"V002__add_email_index.sql": "-- UP\nALTER TABLE customers ADD COLUMN email TEXT;\n-- DOWN\n"})
        self.run_cli("init")
        self.run_cli("up")
        code, _, err = self.run_cli("down", "--to", "V001")
        self.assertEqual(code, 1)
        self.assertIn("empty '-- DOWN'", err)
        self.assertEqual(self.versions(), ["V001", "V002", "V003"])          # V003 was NOT rolled back either
        self.assertEqual(self.run_cli("down")[0], 0)                          # rolling back just V003 is still fine
        self.assertEqual(self.versions(), ["V001", "V002"])

    # ---- dry run and locking
    def test_dry_run_shows_the_sql_and_changes_nothing(self):
        self.run_cli("init")
        code, out, _ = self.run_cli("up", "--dry-run")
        self.assertEqual(code, 0)
        self.assertIn("Nothing was changed", out)
        self.assertIn("CREATE TABLE customers", out)
        self.assertEqual(self.versions(), [])
        self.assertFalse(self.exists("customers"))
        self.run_cli("up")
        out = self.run_cli("down", "--to", "V001", "--dry-run")[1]
        self.assertIn("DROP INDEX customers_email_idx", out)
        self.assertEqual(self.versions(), ["V001", "V002", "V003"])

    def test_only_one_run_may_change_the_database_at_a_time(self):
        self.run_cli("init")
        other = psycopg2.connect(PG_CONN)
        self.addCleanup(other.close)
        with other.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (mm.LOCK_KEY,))      # someone else is mid-migration
        code, _, err = self.run_cli("up")
        self.assertEqual(code, 1)
        self.assertIn("Another mini_migrate run", err)
        self.assertEqual(self.versions(), [])
        with other.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (mm.LOCK_KEY,))
        other.commit()
        self.assertEqual(self.run_cli("up")[0], 0)                            # the lock is released after every run
        self.assertEqual(self.run_cli("down")[0], 0)

    # ---- status and the command line
    def test_status_shows_applied_and_pending(self):
        self.run_cli("init")
        self.run_cli("up", "--to", "V002")
        code, out, _ = self.run_cli("status")
        self.assertEqual(code, 0)
        lines = {line.split()[0]: line for line in out.splitlines() if line.startswith("V0")}
        self.assertIn("applied", lines["V001"])
        self.assertRegex(lines["V002"], r"applied\s+\d{4}-\d\d-\d\d \d\d:\d\d:\d\d")
        self.assertIn("pending", lines["V003"])
        self.assertIn("2 applied, 1 pending", out)

    def test_the_command_line_reports_usage_problems(self):
        saved = os.environ.pop("PG_CONN", None)
        try:
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                self.assertEqual(mm.main(["status"]), 2)
            self.assertIn("PG_CONN", err.getvalue())
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                self.assertEqual(mm.main(["--dsn", "postgresql://postgres:wrong@localhost:1/none", "status"]), 2)
            self.assertIn("Couldn't connect", err.getvalue())
        finally:
            if saved:
                os.environ["PG_CONN"] = saved
        self.run_cli("init")                                                  # (before init, "run init" is the better advice)
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            self.assertEqual(mm.main(["--dsn", PG_CONN, "--migrations-dir", "/no/such/folder", "status"]), 2)
        self.assertIn("doesn't exist", err.getvalue())

    def test_options_work_before_or_after_the_command(self):
        self.run_cli("init")
        for argv in (["--dsn", PG_CONN, "--migrations-dir", self.dir, "status"],
                     ["status", "--dsn", PG_CONN, "--migrations-dir", self.dir]):
            with self.subTest(argv=argv):
                self.assertEqual(mm.main(argv, out=lambda s: None), 0)

    def test_new_then_up_end_to_end(self):
        self.run_cli("init")
        self.run_cli("up")
        self.assertEqual(self.run_cli("new", "add nickname")[0], 0)
        path = Path(self.dir, "V004__add_nickname.sql")
        self.assertTrue(path.exists())
        code, _, err = self.run_cli("up")                                    # an unfilled template can't be applied
        self.assertEqual(code, 1)
        path.write_text("-- UP\nALTER TABLE customers ADD COLUMN nickname TEXT;\n-- DOWN\nALTER TABLE customers DROP COLUMN nickname;\n")
        self.assertEqual(self.run_cli("up")[0], 0)
        self.assertIn("nickname", self.columns("customers"))


if __name__ == "__main__":
    unittest.main()
