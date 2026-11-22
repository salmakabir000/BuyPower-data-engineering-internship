import hashlib
import os
import re
import argparse
import psycopg
from pathlib import Path


MIGRATIONS_DIR = Path("migrations")


def get_connection():
    conn_string = os.getenv("PG_CONN")

    if not conn_string:
        raise RuntimeError("PG_CONN environment variable is not set")

    return psycopg.connect(conn_string)


def initialize():
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS schema_migrations (
        version TEXT PRIMARY KEY,
        description TEXT,
        applied_at TIMESTAMPTZ NOT NULL,
        checksum TEXT NOT NULL
    );
    """

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_table_sql)

    print("Migration tracking table initialized successfully.")


def discover_migrations():
    migrations = []

    for file_path in MIGRATIONS_DIR.glob("*.sql"):
        match = re.match(r"^V(\d+)__(.+)\.sql$", file_path.name)

        if not match:
            raise ValueError(f"Invalid migration filename: {file_path.name}")

        version = f"V{match.group(1)}"
        description = match.group(2).replace("_", " ")

        content = file_path.read_text(encoding="utf-8")

        if "-- UP" not in content or "-- DOWN" not in content:
            raise ValueError(f"Migration must contain UP and DOWN sections: {file_path.name}")

        up_sql, down_sql = content.split("-- DOWN", 1)
        up_sql = up_sql.split("-- UP", 1)[1].strip()
        down_sql = down_sql.strip()

        migrations.append({
            "version": version,
            "description": description,
            "up_sql": up_sql,
            "down_sql": down_sql,
            "file": file_path.name
        })

    migrations.sort(key=lambda migration: int(migration["version"][1:]))
    version_numbers = [
        int(migration["version"][1:])
        for migration in migrations
    ]

    expected_numbers = list(range(1, len(version_numbers) + 1))

    if version_numbers != expected_numbers:
        raise ValueError(
            "Migration version gap detected. "
            f"Found: {version_numbers}, expected: {expected_numbers}"
        )

    return migrations


def list_migrations():
    migrations = discover_migrations()

    if not migrations:
        print("No migration files found.")
        return

    for migration in migrations:
        print(
            f'{migration["file"]} | '
            f'{migration["description"]}'
        )

def show_status():
    migrations = discover_migrations()

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT version, applied_at FROM schema_migrations"
            )
            applied_rows = cursor.fetchall()

    applied = {
        row[0]: row[1]
        for row in applied_rows
    }

    print("\nMigration Status")
    print("-" * 60)

    for migration in migrations:
        version = migration["version"]

        if version in applied:
            status = "APPLIED"
            applied_at = applied[version]
            print(f'{version} | {status} | {applied_at}')
        else:
            print(f'{version} | PENDING')

def calculate_checksum(file_path):
    content = file_path.read_bytes()
    return hashlib.sha256(content).hexdigest()



def apply_migrations(target_version=None):
    migrations = discover_migrations()

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT version, checksum FROM schema_migrations ORDER BY version"
            )
            applied_rows = cursor.fetchall()

    applied = {row[0]: row[1] for row in applied_rows}

    # Check that applied migrations still match their original files.
    for migration in migrations:
        version = migration["version"]

        if version in applied:
            file_path = MIGRATIONS_DIR / migration["file"]
            current_checksum = calculate_checksum(file_path)

            if current_checksum != applied[version]:
                raise RuntimeError(
                    f"Checksum mismatch for {version}. "
                    "An already-applied migration file was modified."
                )

    # Every applied migration must have a corresponding file.
    migration_versions = {
        migration["version"] for migration in migrations
    }

    missing_files = set(applied) - migration_versions

    if missing_files:
        raise RuntimeError(
            f"Applied migration files are missing: {sorted(missing_files)}"
        )

    # Applied migrations must form a continuous prefix.
    ordered_versions = [
        migration["version"] for migration in migrations
    ]

    expected_applied = set(ordered_versions[:len(applied)])

    if set(applied) != expected_applied:
        raise RuntimeError(
            "Out-of-order migration history detected. "
            "Applied migrations must follow version order."
        )

    # Validate the requested target version.
    if target_version is not None:
        if target_version not in ordered_versions:
            raise ValueError(
                f"Unknown target version: {target_version}"
            )

        latest_applied_index = len(applied) - 1
        target_index = ordered_versions.index(target_version)

        if latest_applied_index > target_index:
            raise RuntimeError(
                f"Database is already beyond {target_version}. "
                "Use down --to to roll back."
            )

    # Identify migrations that still need to be applied.
    pending = [
        migration for migration in migrations
        if migration["version"] not in applied
        and (
            target_version is None
            or int(migration["version"][1:]) <= int(target_version[1:])
        )
    ]

    if not pending:
        print("Database is already up to date.")
        return

    # Apply each migration in its own transaction.
    for migration in pending:
        version = migration["version"]
        file_path = MIGRATIONS_DIR / migration["file"]
        checksum = calculate_checksum(file_path)

        print(f"Applying {version}...")

        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(migration["up_sql"])

                cursor.execute(
                    """
                    INSERT INTO schema_migrations
                    (version, description, applied_at, checksum)
                    VALUES (%s, %s, NOW(), %s)
                    """,
                    (version, migration["description"], checksum)
                )

        print(f"Successfully applied {version}.")


def rollback_migration(target_version=None):
    migrations = discover_migrations()

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT version, checksum FROM schema_migrations"
            )
            applied_rows = cursor.fetchall()

    applied = {row[0]: row[1] for row in applied_rows}

    if not applied:
        print("No migrations have been applied.")
        return

    migration_map = {
        migration["version"]: migration
        for migration in migrations
    }

    # Detect applied migrations whose files are missing.
    missing = applied.keys() - migration_map.keys()

    if missing:
        raise RuntimeError(
            f"Applied migration files are missing: {sorted(missing)}"
        )

    # Verify checksums before rollback.
    for version, migration in migration_map.items():
        if version in applied:
            file_path = MIGRATIONS_DIR / migration["file"]
            checksum = calculate_checksum(file_path)

            if checksum != applied[version]:
                raise RuntimeError(
                    f"Checksum mismatch for {version}. Cannot rollback safely."
                )

    applied_migrations = [
        migration for migration in migrations
        if migration["version"] in applied
    ]

    if target_version:
        if target_version not in migration_map:
            raise ValueError(f"Unknown target version: {target_version}")

        if target_version not in applied:
            raise ValueError(
                f"Target version {target_version} has not been applied."
            )

        to_rollback = [
            migration for migration in reversed(applied_migrations)
            if migration["version"] > target_version
        ]
    else:
        to_rollback = list(reversed(applied_migrations[-1:]))

    if not to_rollback:
        print("No migration needs to be rolled back.")
        return

    for migration in to_rollback:
        version = migration["version"]

        print(f"Rolling back {version}...")

        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(migration["down_sql"])

                cursor.execute(
                    "DELETE FROM schema_migrations WHERE version = %s",
                    (version,)
                )

        print(f"Successfully rolled back {version}.")



def create_migration(description):
    # Validate the description.
    if not description.strip():
        raise ValueError("Migration description cannot be empty.")

    # Convert the description into a filename-friendly slug.
    slug = re.sub(r"[^a-z0-9]+", "_", description.lower()).strip("_")

    if not slug:
        raise ValueError("Migration description must contain letters or numbers.")

    # Discover existing migrations to determine the next version.
    migrations = discover_migrations()

    if migrations:
        last_version = max(
            int(migration["version"][1:])
            for migration in migrations
        )
    else:
        last_version = 0

    next_version = f"V{last_version + 1:03d}"

    # Build the migration filename and path.
    filename = f"{next_version}__{slug}.sql"
    file_path = MIGRATIONS_DIR / filename

    # Ensure the migrations directory exists.
    MIGRATIONS_DIR.mkdir(parents=True, exist_ok=True)

    # Prevent accidentally overwriting an existing file.
    if file_path.exists():
        raise FileExistsError(f"Migration already exists: {filename}")

    # Create the migration template.
    template = """-- UP


-- DOWN
"""

    file_path.write_text(template, encoding="utf-8")

    print(f"Created migration: {filename}")



def main():
    parser = argparse.ArgumentParser(
        description="Mini database migration tool"
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True
    )

    subparsers.add_parser("init", help="Initialize migration tracking table")
    subparsers.add_parser("files", help="List discovered migration files")
    subparsers.add_parser("status", help="Show migration status")
    up_parser = subparsers.add_parser("up", help="Apply pending migrations")
    up_parser.add_argument("--to", help="Apply migrations up to a specific version")
    down_parser = subparsers.add_parser("down", help="Rollback the latest migration")
    down_parser.add_argument("--to", help="Rollback until the specified version remains applied")
    new_parser = subparsers.add_parser("new", help="Create a new migration file")
    new_parser.add_argument("description", help="Description of the migration")

    args = parser.parse_args()

    if args.command == "init":
        initialize()
    elif args.command == "files":
        list_migrations()
    elif args.command == "status":
        show_status()
    elif args.command == "up":
        apply_migrations(args.to)
    elif args.command == "down":
        rollback_migration(args.to)
    elif args.command == "new":
        create_migration(args.description)

if __name__ == "__main__":
    main()
