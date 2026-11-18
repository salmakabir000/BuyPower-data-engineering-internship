# Mini PostgreSQL Migration Tool

I built this project to understand how database schema changes are managed without manually updating the database every time something changes.

The tool uses Python and PostgreSQL to manage SQL migration files. It keeps track of which migrations have been applied, allows changes to be rolled back, and checks whether previously applied migration files have been modified.

## What it does

- Finds and validates SQL migration files.
- Applies pending migrations in version order.
- Tracks applied migrations in a PostgreSQL table.
- Rolls back the latest migration or migrations after a specified version.
- Creates new migration files with automatically generated version numbers.
- Uses SHA-256 checksums to detect changes to applied migration files.
- Runs each migration inside a database transaction so failed migrations don't leave partial changes.
- Supports applying migrations up to a specified version.

## Tools used

- Python
- PostgreSQL
- SQL
- Psycopg
- SHA-256

## How migrations work

Migration files are stored in the `migrations/` directory and follow this naming format:

`V001__create_customers.sql`

Each file contains two sections:

- `UP`: applies the database change.
- `DOWN`: reverses the database change.

For example:

```sql
-- UP
ALTER TABLE customers
ADD COLUMN city TEXT;

-- DOWN
ALTER TABLE customers
DROP COLUMN city;
```

## Example migrations

I created five migrations to test the tool:

| Version | Change |
|---|---|
| V001 | Created the customers table |
| V002 | Added an email column and index |
| V003 | Added a phone column |
| V004 | Added a city column |
| V005 | Created an index on created_at |

## Running the tool

Set the PostgreSQL connection string:

```bash
export PG_CONN="postgresql://username:password@localhost:5432/database_name"
```

Install the dependency:

```bash
pip install -r requirements.txt
```

Initialize the migration tracking table:

```bash
python mini_migrate.py init
```

Check migration status:

```bash
python mini_migrate.py status
```

Apply all pending migrations:

```bash
python mini_migrate.py up
```

Apply migrations up to a particular version:

```bash
python mini_migrate.py up --to V003
```

Roll back the latest migration:

```bash
python mini_migrate.py down
```

Roll back to a particular version:

```bash
python mini_migrate.py down --to V001
```

Create a migration:

```bash
python mini_migrate.py new "add address column"
```

## What I tested

- Applying all five migrations successfully.
- Running the apply command again without duplicating applied migrations.
- Rolling back the latest migration.
- Rolling back to a specific version.
- Detecting changes to previously applied migration files using checksums.
- Rejecting missing or out-of-order migration versions.
- Confirming that a failed migration is rolled back and not recorded as applied.

## A note on migration files

Once a migration has been applied, its SQL file should not be edited. New database changes should be introduced through new migration files instead.
