#!/usr/bin/env python3
"""
mini_migrate.py - a small database migration tool for Postgres ("schema as code").

    mini_migrate init                          create the schema_migrations table
    mini_migrate status                        show applied and pending migrations
    mini_migrate up                            apply all pending migrations
    mini_migrate up --to V002                  apply up to (and including) V002
    mini_migrate down                          roll back the latest migration
    mini_migrate down --to V001                roll back until V001 is the latest
    mini_migrate new "add phone column"        create the next migration file
    mini_migrate diff LOCAL_DSN TARGET_DSN     SQL that turns LOCAL's schema into TARGET's

The database comes from --dsn or the PG_CONN environment variable, for example
    export PG_CONN=postgresql://postgres:password@localhost:5432/mydb

Each migration is a file in migrations/ named V<number>__<description>.sql with two sections:

    -- UP
    CREATE TABLE customers (id SERIAL PRIMARY KEY, name TEXT NOT NULL);

    -- DOWN
    DROP TABLE customers;

Safety rules (each one stops the run and explains why):
  1. Migrations are applied in strict version order. If V003 is applied but V002 is
     not, nothing runs.
  2. Every migration file is hashed when applied. If an applied migration's file
     later changes, nothing runs. (Never edit an applied migration; add a new one.)
  3. Each migration runs in one transaction together with its bookkeeping row, so if
     it fails part-way the database is left exactly as it was.
"""

import argparse
import hashlib
import os
import re
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, timezone
from pathlib import Path

import psycopg2

FILE_RE = re.compile(r"^V(\d+)__(.+)\.sql$")
UP_RE = re.compile(r"^\s*--\s*UP\s*$", re.IGNORECASE)
DOWN_RE = re.compile(r"^\s*--\s*DOWN\s*$", re.IGNORECASE)
DOLLAR_QUOTED_RE = re.compile(r"\$(\w*)\$.*?\$\1\$", re.DOTALL)
STRING_RE = re.compile(r"'(?:[^']|'')*'")
TX_RE = re.compile(
    r"^\s*(BEGIN|COMMIT|ROLLBACK|START\s+TRANSACTION)(\s+(TRANSACTION|WORK))?\s*(;|$)",
    re.IGNORECASE | re.MULTILINE,
)
LOCK_KEY = 7_284_513_001  # every mini_migrate run on a database uses this advisory lock

TABLE_DDL = (
    "CREATE TABLE IF NOT EXISTS schema_migrations ("
    "version TEXT PRIMARY KEY, "
    "description TEXT, "
    "applied_at TIMESTAMPTZ NOT NULL, "
    "checksum TEXT NOT NULL)"
)

NEW_TEMPLATE = """-- Migration {version}: {description}
-- Created {created}. Once this has been applied anywhere, never edit it:
-- put any further change in a new migration.

-- UP
-- Write the SQL that applies this change.

-- DOWN
-- Write the SQL that undoes it (leave this section empty if it can't be undone).
"""


class MigrationError(Exception):
    """A safety rule was triggered, or a migration failed. The message explains what to do."""


class ConfigError(Exception):
    """Something is wrong with how the tool was called (no database, no folder...)."""


# ---------------------------------------------------------------- small helpers

def version_name(number):
    return f"V{number:03d}"


def parse_version(text):
    """'V002', 'v2' and '2' all mean version 2."""
    m = re.fullmatch(r"[Vv]?(\d+)", str(text).strip())
    if not m:
        raise MigrationError(f"'{text}' isn't a version number. Use something like V002 (or just 2).")
    return int(m.group(1))


def number_of(version):
    m = re.fullmatch(r"V(\d+)", version)
    return int(m.group(1)) if m else 10 ** 12  # unknown names sort last


def checksum(text):
    """SHA-256 of the file's content. Line endings are normalised so a Windows/Linux
    difference can't look like an edit."""
    return hashlib.sha256(text.replace("\r\n", "\n").encode("utf-8")).hexdigest()


def has_sql(sql):
    return any(line.strip() and not line.strip().startswith("--") for line in sql.splitlines())


def slugify(text):
    slug = re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")[:60].strip("_")
    if not slug:
        raise MigrationError("Give the migration a description, for example: mini_migrate new \"add phone column\"")
    return slug


def contains_transaction_control(sql):
    """True if the SQL has its own BEGIN/COMMIT/ROLLBACK (which would break the
    one-transaction-per-migration guarantee). Text inside strings, dollar-quoted
    function bodies and comments is ignored."""
    cleaned = DOLLAR_QUOTED_RE.sub("", sql)
    cleaned = STRING_RE.sub("", cleaned)
    cleaned = re.sub(r"--[^\n]*", "", cleaned)
    return bool(TX_RE.search(cleaned))


# ---------------------------------------------------------------- reading migration files

@dataclass(frozen=True)
class Migration:
    number: int
    description: str
    filename: str
    up_sql: str
    down_sql: str
    checksum: str

    @property
    def version(self):
        return version_name(self.number)

    @property
    def reversible(self):
        return has_sql(self.down_sql)


def parse_migration(filename, text):
    m = FILE_RE.match(filename)
    if not m:
        raise MigrationError(f"'{filename}' doesn't match the pattern V<number>__<description>.sql (for example V001__create_customers.sql)")
    number, description = int(m.group(1)), m.group(2).replace("_", " ")

    sections, current = {}, None
    for line in text.splitlines():
        if UP_RE.match(line) or DOWN_RE.match(line):
            name = "UP" if UP_RE.match(line) else "DOWN"
            if name in sections:
                raise MigrationError(f"{filename} has two '-- {name}' sections; it must have exactly one")
            sections[name] = []
            current = sections[name]
        elif current is not None:
            current.append(line)
    for name in ("UP", "DOWN"):
        if name not in sections:
            raise MigrationError(
                f"{filename} has no '-- {name}' section. Every migration needs both "
                "(leave '-- DOWN' empty if it can't be undone)")
    up_sql, down_sql = "\n".join(sections["UP"]).strip(), "\n".join(sections["DOWN"]).strip()
    if not has_sql(up_sql):
        raise MigrationError(f"{filename} has an empty '-- UP' section, so there is nothing to apply")
    for name, sql in (("UP", up_sql), ("DOWN", down_sql)):
        if contains_transaction_control(sql):
            raise MigrationError(
                f"{filename} ('-- {name}') contains BEGIN/COMMIT/ROLLBACK. Remove it: mini_migrate already runs "
                "every migration in its own transaction, and doing it twice would break the all-or-nothing guarantee")
    return Migration(number, description, filename, up_sql, down_sql, checksum(text))


def load_migrations(directory):
    folder = Path(directory)
    if not folder.is_dir():
        raise ConfigError(
            f"The migrations folder '{directory}' doesn't exist. Run mini_migrate from your project folder, "
            "use --migrations-dir, or run `mini_migrate init` to create it.")
    migrations, seen = [], {}
    for path in sorted(folder.glob("*.sql")):
        migration = parse_migration(path.name, path.read_text(encoding="utf-8"))
        if migration.number in seen:
            raise MigrationError(f"{seen[migration.number]} and {path.name} both use version {migration.version}. "
                                 "Each version number may only be used once")
        seen[migration.number] = path.name
        migrations.append(migration)
    return sorted(migrations, key=lambda m: m.number)


# ---------------------------------------------------------------- reading the database

def table_exists(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('schema_migrations') IS NOT NULL")
        exists = cur.fetchone()[0]
    conn.commit()
    return exists


def ensure_initialized(conn):
    if not table_exists(conn):
        raise MigrationError("The schema_migrations table doesn't exist in this database yet. Run `mini_migrate init` first.")


def read_applied(conn):
    """{version: {'description', 'applied_at', 'checksum'}} for everything recorded as applied."""
    with conn.cursor() as cur:
        cur.execute("SELECT version, description, applied_at, checksum FROM schema_migrations")
        rows = cur.fetchall()
    conn.commit()
    return {v: {"description": d, "applied_at": a, "checksum": c} for v, d, a, c in rows}


# ---------------------------------------------------------------- the safety rules

@dataclass
class Entry:
    version: str
    description: str
    state: str          # applied | pending | CHANGED | MISSING | OUT-OF-ORDER
    applied_at: object


@dataclass
class Analysis:
    entries: list
    problems: list


def analyze(migrations, applied, folder="migrations"):
    """Compares the files on disk with what the database says is applied."""
    by_version = {m.version: m for m in migrations}
    applied_numbers = sorted(number_of(v) for v in applied)
    latest = applied_numbers[-1] if applied_numbers else None
    entries, problems = [], []

    for version in sorted(applied, key=number_of):
        row, m = applied[version], by_version.get(version)
        if m is None:
            state = "MISSING"
            problems.append(
                f"{version} ('{row['description']}') is recorded as applied, but there is no file for it in {folder}/. "
                "Restore the file: without it the migration can't be verified or rolled back.")
        elif m.checksum != row["checksum"]:
            state = "CHANGED"
            problems.append(
                f"{version} ('{m.description}') was edited after it was applied (recorded checksum "
                f"{row['checksum'][:12]}, file now {m.checksum[:12]}). Never edit an applied migration: restore the "
                f"original file (for example with `git checkout {folder}/{m.filename}`) and put the change in a new migration.")
        else:
            state = "applied"
        entries.append(Entry(version, row["description"], state, row["applied_at"]))

    for m in migrations:
        if m.version in applied:
            continue
        if latest is not None and m.number < latest:
            state = "OUT-OF-ORDER"
            before = [n for n in applied_numbers if n < m.number]
            fix = (f"`mini_migrate down --to {version_name(before[-1])}`" if before
                   else "`mini_migrate down` (repeat it until nothing newer than this is applied)")
            problems.append(
                f"{m.version} ('{m.description}') is still pending, but {version_name(latest)} is already applied. "
                f"Migrations must be applied in version order, so it won't be applied now. To fix it, roll the newer "
                f"migrations back with {fix}, then run `mini_migrate up` so everything is applied in order. If {m.version} "
                f"arrived late (for example from a merge), renumber its file.")
        else:
            state = "pending"
        entries.append(Entry(m.version, m.description, state, None))

    entries.sort(key=lambda e: number_of(e.version))
    return Analysis(entries, problems)


def refuse(message, problems):
    raise MigrationError(message + "\n" + "\n".join(f"  - {p}" for p in problems))


def plan_up(migrations, applied, folder="migrations", to=None):
    """The migrations to apply, in order. Refuses if anything is inconsistent."""
    analysis = analyze(migrations, applied, folder)
    if analysis.problems:
        refuse("Refusing to apply migrations, because:", analysis.problems)
    pending = [m for m in migrations if m.version not in applied]
    if to is not None:
        target = parse_version(to)
        if not any(m.number == target for m in migrations):
            raise MigrationError(f"There is no migration file for {version_name(target)}")
        pending = [m for m in pending if m.number <= target]
    return pending


def plan_down(migrations, applied, folder="migrations", to=None):
    """The migrations to roll back, newest first. Only the migrations about to be undone
    are checked: their DOWN must still match what their UP did."""
    if not applied:
        return []
    by_version = {m.version: m for m in migrations}
    versions = sorted(applied, key=number_of)
    if to is None:
        chosen = [versions[-1]]
    else:
        target = parse_version(to)
        if version_name(target) not in applied:
            raise MigrationError(f"{version_name(target)} isn't applied, so you can't roll back to it. "
                                 f"Applied: {', '.join(versions)}")
        chosen = [v for v in versions if number_of(v) > target][::-1]

    problems = []
    for version in chosen:
        m = by_version.get(version)
        if m is None:
            problems.append(f"{version} has no file in {folder}/, so its DOWN section can't be run. Restore the file.")
        elif m.checksum != applied[version]["checksum"]:
            problems.append(f"{version} was edited after it was applied, so its DOWN section may no longer undo what was "
                            f"actually applied. Restore the original file (`git checkout {folder}/{m.filename}`).")
        elif not m.reversible:
            problems.append(f"{version} ('{m.description}') has an empty '-- DOWN' section: it can't be rolled back.")
    if problems:
        refuse("Refusing to roll back, because (nothing was changed):", problems)
    return [by_version[v] for v in chosen]


# ---------------------------------------------------------------- running migrations

@contextmanager
def migration_lock(conn):
    """Only one mini_migrate run may change a database at a time."""
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_KEY,))
        got = cur.fetchone()[0]
    conn.commit()
    if not got:
        raise MigrationError("Another mini_migrate run is already changing this database. "
                             "Wait for it to finish, then try again.")
    try:
        yield
    finally:
        try:
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", (LOCK_KEY,))
            conn.commit()
        except psycopg2.Error:
            pass  # the connection is gone, and the lock went with it


def run_migrations(conn, plan, direction, out=print):
    """Runs each migration in its OWN transaction, together with its bookkeeping row:
    either both happen or neither does."""
    verb = "applying" if direction == "up" else "rolling back"
    for m in plan:
        out(f"  {verb} {m.version} {m.description} ...")
        try:
            with conn.cursor() as cur:
                if direction == "up":
                    cur.execute(m.up_sql)
                    cur.execute(
                        "INSERT INTO schema_migrations (version, description, applied_at, checksum) "
                        "VALUES (%s, %s, now(), %s)", (m.version, m.description, m.checksum))
                else:
                    cur.execute(m.down_sql)
                    cur.execute("DELETE FROM schema_migrations WHERE version = %s", (m.version,))
            conn.commit()
        except psycopg2.Error as e:
            conn.rollback()
            raise MigrationError(
                f"{m.version} ({m.description}) failed, and its transaction was rolled back, so the database is exactly as "
                f"it was before this migration.\n  Postgres said: {str(e).strip()}\n"
                "Migrations that finished earlier in this run stay applied. Fix the problem and run the command again.") from e
        except BaseException:
            conn.rollback()
            raise


def show_plan(plan, direction, out=print):
    word = "apply" if direction == "up" else "roll back"
    out(f"Dry run: would {word} {len(plan)} migration(s). Nothing was changed.")
    for m in plan:
        out(f"\n--- {m.version} {m.description} ---")
        out("\n".join("    " + line for line in (m.up_sql if direction == "up" else m.down_sql).splitlines()))


# ---------------------------------------------------------------- commands

def db_name(conn):
    return conn.get_dsn_parameters().get("dbname", "?")


def cmd_init(conn, directory, out=print):
    existed = table_exists(conn)
    with conn.cursor() as cur:
        cur.execute(TABLE_DDL)
    conn.commit()
    folder = Path(directory)
    made_folder = not folder.exists()
    folder.mkdir(parents=True, exist_ok=True)
    out(f"schema_migrations already exists in '{db_name(conn)}'." if existed
        else f"Created the schema_migrations table in '{db_name(conn)}'.")
    if made_folder:
        out(f"Created the '{directory}/' folder. Add migrations with: mini_migrate new \"describe the change\"")
    return 0


def format_time(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S") if dt else "-"


def cmd_status(conn, directory, out=print):
    ensure_initialized(conn)
    analysis = analyze(load_migrations(directory), read_applied(conn), directory)
    out(f"Database: {db_name(conn)}")
    if not analysis.entries:
        out("No migrations yet. Create one with: mini_migrate new \"describe the change\"")
        return 0
    out(f"{'VERSION':<8} {'STATE':<13} {'APPLIED AT (UTC)':<19}  DESCRIPTION")
    for e in analysis.entries:
        out(f"{e.version:<8} {e.state:<13} {format_time(e.applied_at):<19}  {e.description}")
    n_applied = sum(1 for e in analysis.entries if e.state in ("applied", "CHANGED", "MISSING"))
    n_pending = sum(1 for e in analysis.entries if e.state == "pending")
    n_out_of_order = sum(1 for e in analysis.entries if e.state == "OUT-OF-ORDER")
    out(f"\n{n_applied} applied, {n_pending} pending" + (f", {n_out_of_order} out of order" if n_out_of_order else "") + ".")
    if analysis.problems:
        out("\nPROBLEMS (nothing will be applied until these are fixed):")
        for p in analysis.problems:
            out(f"  - {p}")
        return 1
    return 0


def cmd_up(conn, directory, to=None, dry_run=False, out=print):
    ensure_initialized(conn)
    migrations = load_migrations(directory)
    with migration_lock(conn):
        applied = read_applied(conn)
        plan = plan_up(migrations, applied, directory, to)
        if not plan:
            out("Nothing to apply: the database is already up to date." if to is None
                else f"Nothing to apply: nothing pending at or before {version_name(parse_version(to))}.")
            return 0
        if dry_run:
            show_plan(plan, "up", out)
            return 0
        out(f"Applying {len(plan)} migration(s) to '{db_name(conn)}':")
        run_migrations(conn, plan, "up", out)
        latest = max(number_of(v) for v in read_applied(conn))
    out(f"Done. The database is now at {version_name(latest)}.")
    return 0


def cmd_down(conn, directory, to=None, dry_run=False, out=print):
    ensure_initialized(conn)
    migrations = load_migrations(directory)
    with migration_lock(conn):
        applied = read_applied(conn)
        plan = plan_down(migrations, applied, directory, to)
        if not plan:
            out("Nothing to roll back." if not applied else f"Already at {version_name(parse_version(to))}: nothing to roll back.")
            return 0
        if dry_run:
            show_plan(plan, "down", out)
            return 0
        out(f"Rolling back {len(plan)} migration(s) in '{db_name(conn)}':")
        run_migrations(conn, plan, "down", out)
        remaining = read_applied(conn)
    out(f"Done. The database is now at {version_name(max(number_of(v) for v in remaining))}." if remaining
        else "Done. No migrations are applied now.")
    return 0


def next_number(directory):
    folder = Path(directory)
    numbers = [int(FILE_RE.match(p.name).group(1)) for p in folder.glob("*.sql") if FILE_RE.match(p.name)]
    return max(numbers, default=0) + 1


def write_migration(directory, description, up_sql=None, down_sql=None):
    """Creates the next numbered migration file and returns its path."""
    folder = Path(directory)
    folder.mkdir(parents=True, exist_ok=True)
    number = next_number(directory)
    path = folder / f"{version_name(number)}__{slugify(description)}.sql"
    if up_sql is None:
        content = NEW_TEMPLATE.format(version=version_name(number), description=description.strip(), created=date.today())
    else:
        content = (f"-- Migration {version_name(number)}: {description.strip()}\n"
                   f"-- Generated by `mini_migrate diff` on {date.today()}. Review it before applying: a column that was\n"
                   "-- renamed shows up as a DROP plus an ADD, which would lose the data.\n\n"
                   f"-- UP\n{up_sql}\n\n-- DOWN\n{down_sql}\n")
    with open(path, "x", encoding="utf-8") as f:  # 'x' refuses to overwrite an existing file
        f.write(content)
    return path


def cmd_new(directory, description, out=print):
    path = write_migration(directory, description)
    out(f"Created {path}")
    out("Write the UP and DOWN SQL in it, then run: mini_migrate up")
    return 0


def cmd_diff(local_dsn, target_dsn, directory, write=None, out=print):
    from schema_diff import diff, introspect, render_sql

    local, target = connect(local_dsn), connect(target_dsn)
    try:
        a, b = introspect(local), introspect(target)
    finally:
        local.close()
        target.close()
    up, down = diff(a, b), diff(b, a)
    if not up:
        out("-- No differences: the two schemas already match.")
        return 0
    if write:
        path = write_migration(directory, write, render_sql(up), render_sql(down))
        out(f"Created {path} with the UP (local -> target) and DOWN (target -> local) SQL. Review it, then run: mini_migrate up")
    else:
        out("-- SQL that turns the LOCAL schema into the TARGET schema:\n" + render_sql(up))
    return 0


# ---------------------------------------------------------------- command line

def connect(dsn=None):
    dsn = dsn or os.environ.get("PG_CONN")
    if not dsn:
        raise ConfigError("No database given. Set PG_CONN (export PG_CONN=postgresql://user:password@localhost:5432/dbname) "
                          "or use --dsn.")
    try:
        return psycopg2.connect(dsn, connect_timeout=10)
    except psycopg2.Error as e:
        raise ConfigError(f"Couldn't connect to the database: {str(e).strip()}\nIs Postgres running? Try: sudo docker start pg-cdc") from e


def common_options():
    """--dsn and --migrations-dir. Every parser gets its OWN copy: argparse shares option objects
    between parsers, so one copy used everywhere would let a default set on the main parser
    overwrite what you typed before the command."""
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--dsn", default=argparse.SUPPRESS, help="database connection string (default: $PG_CONN)")
    common.add_argument("--migrations-dir", default=argparse.SUPPRESS,
                        help="folder with the migration files (default: migrations)")
    return common


def build_parser():
    parser = argparse.ArgumentParser(prog="mini_migrate", description="A small database migration tool for Postgres.",
                                     parents=[common_options()])
    parser.set_defaults(dsn=None, migrations_dir="migrations")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", parents=[common_options()], help="create the schema_migrations table")
    sub.add_parser("status", parents=[common_options()], help="show applied and pending migrations")
    for name, text in (("up", "apply pending migrations"), ("down", "roll back migrations")):
        s = sub.add_parser(name, parents=[common_options()], help=text)
        s.add_argument("--to", metavar="V002", help=("apply up to and including this version" if name == "up"
                                                    else "roll back until this version is the latest applied"))
        s.add_argument("--dry-run", action="store_true", help="show what would run, change nothing")
    s = sub.add_parser("new", parents=[common_options()], help="create the next migration file")
    s.add_argument("description")
    s = sub.add_parser("diff", parents=[common_options()],
                       help="SQL that turns LOCAL's schema into TARGET's (stretch goal)")
    s.add_argument("local_dsn")
    s.add_argument("target_dsn")
    s.add_argument("--write", metavar="DESCRIPTION", help="save the result as the next migration file")
    return parser


def main(argv=None, out=print):
    args = build_parser().parse_args(argv)
    conn = None
    try:
        if args.command == "new":
            return cmd_new(args.migrations_dir, args.description, out)
        if args.command == "diff":
            return cmd_diff(args.local_dsn, args.target_dsn, args.migrations_dir, args.write, out)
        conn = connect(args.dsn)
        if args.command == "init":
            return cmd_init(conn, args.migrations_dir, out)
        if args.command == "status":
            return cmd_status(conn, args.migrations_dir, out)
        if args.command == "up":
            return cmd_up(conn, args.migrations_dir, args.to, args.dry_run, out)
        return cmd_down(conn, args.migrations_dir, args.to, args.dry_run, out)
    except MigrationError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except ConfigError as e:
        print(f"Config error: {e}", file=sys.stderr)
        return 2
    except psycopg2.Error as e:
        print(f"Database error: {str(e).strip()}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nInterrupted. The migration that was running was rolled back.", file=sys.stderr)
        return 130
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())
