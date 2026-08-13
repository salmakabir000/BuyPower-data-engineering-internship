import click
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import os
import glob
import json
from datetime import datetime, timezone

TARGET_SIZE_MB = 96  # aim for the middle of 64-128MB range
MAX_SIZE_MB = 128

def get_dir_size_mb(files):
    total = sum(os.path.getsize(f) for f in files)
    return total / (1024 * 1024)

def write_partition(table, partition_path, start_index=1):
    """Write a table to a partition, splitting into multiple parts if too large."""
    os.makedirs(partition_path, exist_ok=True)

    total_rows = table.num_rows
    if total_rows == 0:
        return

    # Estimate size and decide on number of splits
    est_size_mb = table.nbytes / (1024 * 1024)
    num_parts = max(1, int(est_size_mb / MAX_SIZE_MB) + 1)
    rows_per_part = (total_rows // num_parts) + 1

    for i in range(num_parts):
        start = i * rows_per_part
        end = min(start + rows_per_part, total_rows)
        if start >= end:
            continue
        chunk = table.slice(start, end - start)
        part_num = start_index + i
        part_path = os.path.join(partition_path, f"part-{part_num:03d}.parquet")
        pq.write_table(chunk, part_path, compression='snappy')
        print(f"  Wrote {part_path} ({chunk.num_rows:,} rows, {chunk.nbytes/(1024*1024):.1f} MB)")


@click.group()
def cli():
    pass


@cli.command()
@click.argument('input_file')
@click.option('--dataset', required=True, help='Dataset name (e.g. trips)')
def ingest(input_file, dataset):
    """Read a parquet file and partition it by year/month/day."""
    print(f"Reading {input_file}...")
    table = pq.read_table(input_file)

    pickup_col = 'tpep_pickup_datetime'
    if pickup_col not in table.column_names:
        print(f"Error: column {pickup_col} not found")
        return

    # Extract year, month, day
    dates = table.column(pickup_col)
    years = pc.year(dates)
    months = pc.month(dates)
    days = pc.day(dates)

    table = table.append_column('_year', years)
    table = table.append_column('_month', months)
    table = table.append_column('_day', days)

    df_keys = table.select(['_year', '_month', '_day']).to_pandas()
    unique_partitions = df_keys.drop_duplicates().values.tolist()

    print(f"Found {len(unique_partitions)} unique day partitions")

    base_path = os.path.join('data_lake', dataset)

    for year, month, day in unique_partitions:
        # Filter rows for this exact partition
        mask = pc.and_(
            pc.and_(pc.equal(table.column('_year'), year), pc.equal(table.column('_month'), month)),
            pc.equal(table.column('_day'), day)
        )
        partition_table = table.filter(mask)
        # Drop helper columns before writing
        partition_table = partition_table.drop(['_year', '_month', '_day'])

        partition_path = os.path.join(
            base_path,
            f"year={year:04d}",
            f"month={month:02d}",
            f"day={day:02d}"
        )

        # Check for existing part files (late-arriving data / re-ingest)
        existing_parts = sorted(glob.glob(os.path.join(partition_path, "part-*.parquet")))
        start_index = len(existing_parts) + 1

        write_partition(partition_table, partition_path, start_index)

    print(f"\nIngest complete. Data written to {base_path}/")


@cli.command()
@click.argument('dataset')
@click.option('--year', required=True, help='Year to compact')
@click.option('--month', required=True, help='Month to compact')
def compact(dataset, year, month):
    """Merge small part files into fewer, larger files per partition."""
    base_path = os.path.join('data_lake', dataset, f"year={year}", f"month={month.zfill(2)}")

    if not os.path.exists(base_path):
        print(f"No data found at {base_path}")
        return

    day_dirs = sorted(glob.glob(os.path.join(base_path, "day=*")))
    print(f"Found {len(day_dirs)} day partitions to compact")

    for day_dir in day_dirs:
        part_files = sorted(glob.glob(os.path.join(day_dir, "part-*.parquet")))
        if len(part_files) <= 1:
            print(f"{day_dir}: already compact ({len(part_files)} file)")
            continue

        print(f"{day_dir}: compacting {len(part_files)} files...")
        tables = [pq.read_table(f) for f in part_files]
        merged = pa.concat_tables(tables)

        # Delete old part files
        for f in part_files:
            os.remove(f)

        write_partition(merged, day_dir, start_index=1)
        print(f"  Compacted into fewer file(s)")

    print("\nCompaction complete.")


if __name__ == '__main__':
    cli()
