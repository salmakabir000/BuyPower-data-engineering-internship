# Database Migration Tool

A small command-line migration tool for PostgreSQL.

## Requirements
- Python 3.9+
- PostgreSQL
- `PG_CONN` environment variable containing your PostgreSQL connection string

Install:

```powershell
python -m pip install -r requirements.txt
$env:PG_CONN = "host=localhost port=5432 dbname=YOUR_DATABASE user=postgres password=YOUR_PASSWORD"
```

Replace the example database details with your own.

## Migration file format

Migration files go in `migrations/` and use names such as `V001__create_customers.sql`.
Each file must contain both sections:

```sql
-- UP
CREATE TABLE customers (id SERIAL PRIMARY KEY, name TEXT NOT NULL);

-- DOWN
DROP TABLE customers;
```

Never edit a migration after it has been applied. Create a new migration instead.

## Commands

```powershell
python mini_migrate.py init
python mini_migrate.py status
python mini_migrate.py up
python mini_migrate.py up --to V002
python mini_migrate.py down
python mini_migrate.py down --to V001
python mini_migrate.py new "add phone column"
```

`init` creates the `schema_migrations` table. `status` shows applied and pending files.
`up` applies migrations in order. `up --to` applies through the selected version.
`down` rolls back the latest migration. `down --to` rolls back everything newer than the target.
`new` creates the next numbered migration file.

## Safety
- Each migration and its tracking-table update run in one transaction.
- A failed migration is rolled back and is not recorded as applied.
- SHA-256 checksums detect edits to previously applied migration files.
- The tool rejects applied migration history with an earlier version missing.
- Rollback SQL and its tracking-row deletion also run in one transaction.

## Tests

```powershell
python -m unittest discover -s tests -v
```

The unit tests do not require a running PostgreSQL server. A real database connection is needed to run the CLI migration commands.
