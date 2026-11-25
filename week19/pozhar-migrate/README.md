# mini_migrate - a small database migration tool for Postgres

"Schema as code": every change to the database is a numbered SQL file in version control.
`mini_migrate` applies them in order, remembers which ones have run, can roll them back,
and refuses to continue if something looks wrong. It is a miniature Alembic or Flyway.

## Quick start

    pip install -r requirements.txt
    sudo docker exec -it pg-cdc psql -U postgres -c "CREATE DATABASE migrate_demo;"
    export PG_CONN=postgresql://postgres:password@localhost:5432/migrate_demo

    python3 mini_migrate.py init        # create the schema_migrations table
    python3 mini_migrate.py up          # apply every pending migration
    python3 mini_migrate.py status      # what is applied, what is pending

## Commands

    mini_migrate init                      create schema_migrations (and the migrations/ folder if missing)
    mini_migrate status                    show applied and pending migrations (exit code 1 if there are problems)
    mini_migrate up                        apply all pending migrations, in order
    mini_migrate up --to V002              apply up to and including V002
    mini_migrate down                      roll back the latest migration
    mini_migrate down --to V001            roll back until V001 is the latest applied
    mini_migrate new "add phone column"    create the next numbered migration file
    mini_migrate diff LOCAL_DSN TARGET_DSN SQL that turns LOCAL's schema into TARGET's (see below)

`up` and `down` also take `--dry-run` (print the SQL, change nothing). The database comes from
`--dsn` or `$PG_CONN`, and the folder from `--migrations-dir` (default `migrations`). Both options
work before or after the command.

## A migration file

`migrations/V003__add_phone_column.sql`, named `V<number>__<description>.sql`:

    -- UP
    ALTER TABLE customers ADD COLUMN phone TEXT;

    -- DOWN
    ALTER TABLE customers DROP COLUMN phone;

Versions are compared as numbers (V010 comes after V002). An empty `-- DOWN` section means the
migration can't be undone, and `down` will refuse to go past it. Don't put BEGIN or COMMIT in a
migration: the tool already runs each one in its own transaction, and a second one would break
the all-or-nothing guarantee (the tool rejects such files).

## The three safety rules

1. **Strict order.** Migrations run in version order. If V003 is applied but V002 is not,
   `up` refuses and explains how to fix it (roll back to the last good version with
   `down --to`, then `up` again so everything is applied in order).
2. **Checksums.** Each migration's file is hashed (SHA-256) when it is applied. If an applied
   migration's file later changes, `up` and `status` refuse and name the file. Line-ending
   differences (Windows vs Linux) don't count as a change.
3. **One transaction each.** A migration and its row in `schema_migrations` are committed
   together, so if the SQL fails part-way the database is exactly as it was. Postgres can roll
   back DDL (CREATE TABLE, ALTER TABLE...), which is what makes this possible. Earlier
   migrations in the same run stay applied.

Also: a database lock stops two runs from changing the same database at once, and `down` only
checks the migrations it is about to undo, so you can always roll back to get out of a mess.

## Why you should never edit an applied migration

A migration is a record of something that has **already happened** to real databases: yours,
your teammates', staging, production. Editing the file does not change any of those
databases, because they already ran the old version. Now the file says one thing and the
databases say another.

Example: V003 adds `phone TEXT`. Later you decide the column should be `NOT NULL`, so you edit
V003 to say `phone TEXT NOT NULL`. Your own database never re-runs V003, so your phone column is
still nullable. A teammate who sets up a fresh database gets a NOT NULL column. Production has
whatever it had when V003 ran. Three databases, three different schemas, all "at V003". This
is schema drift, and it causes the worst kind of bug: code that works on one machine and fails
on another, with nothing in the history to explain why.

That is why the checksum rule exists: it turns a silent divergence into an immediate, explained
error. The right way to make that change is a **new migration** (V007:
`ALTER TABLE customers ALTER COLUMN phone SET NOT NULL`), which every database applies exactly once,
in order. Think of a bank ledger: you never erase an old entry, you add a correcting one.

The one safe exception: a migration that has not been applied anywhere shared (it only exists on
your machine or branch). Roll it back with `down`, edit it, apply it again.

## Try the safety rules yourself

    cp -r migrations /tmp/m && export M="python3 mini_migrate.py --migrations-dir /tmp/m"
    $M init && $M up && $M status                       # 6 applied

    echo "-- sneaky" >> /tmp/m/V001__create_customers.sql
    $M up                                               # refuses: V001 was edited after it was applied
    sed -i '$ d' /tmp/m/V001__create_customers.sql      # undo the edit

    $M down --to V001                                   # out-of-order: apply V003+ without V002
    mv /tmp/m/V002__add_email_index.sql /tmp/held.sql
    $M up                                               #   (V003-V006 run, because V002's file is missing)
    mv /tmp/held.sql /tmp/m/V002__add_email_index.sql
    $M up                                               # refuses: V002 pending but V006 already applied
    $M status                                           # V002 shows OUT-OF-ORDER
    $M down --to V001 && $M up                          # the fix

## Auto-generating a migration from two databases (stretch goal)

    python3 mini_migrate.py diff "$LOCAL_DSN" "$TARGET_DSN"
    python3 mini_migrate.py diff "$LOCAL_DSN" "$TARGET_DSN" --write "sync with target"

It compares the two schemas and prints the SQL that turns LOCAL into TARGET. With `--write`
it saves that as the next migration, with the **reverse** SQL as its DOWN section.
It handles tables, columns (type, NOT NULL, default, SERIAL, identity, generated), primary
key / unique / foreign key / check constraints, and indexes. Statements are ordered so they run
safely (drop old constraints, create tables, add columns, add constraints, and only then the
destructive drops). It needs Postgres 12 or newer.

**Always read the output.** It can't see renames: a renamed column looks like a drop plus an
add, which would lose the data. It also ignores views, functions, triggers, enums and other
custom types, and column order.

## Tests

    sudo docker exec -it pg-cdc psql -U postgres -c "CREATE DATABASE migrate_test;"
    export PG_CONN=postgresql://postgres:password@localhost:5432/migrate_test
    python3 -m unittest test_mini_migrate test_schema_diff

49 tests against a real Postgres. They **wipe the public schema** of `PG_CONN` and create/drop two
extra databases, so they refuse to run unless the database name contains "test".

## Known limitations

* Some statements can't run inside a transaction (`CREATE INDEX CONCURRENTLY`, and on older
  Postgres `ALTER TYPE ... ADD VALUE`). Postgres will say so; run those by hand.
* `down` is only as good as the DOWN section you wrote. Rolling back a migration that dropped a
  column can't bring the data back.
* Only Postgres, and only the `public` schema in the diff.
* The migration lock guards against two runs of this tool, not against other programs changing
  the schema.
