import argparse
from pathlib import Path
import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

TARGET_SIZE = 128 * 1024 * 1024


def write_partition(day_df, partition_path):
    existing_parts = list(partition_path.glob("part-*.parquet"))
    part_number = len(existing_parts) + 1
    sample_size = min(len(day_df), 10_000)
    sample = day_df.head(sample_size)

    buffer = io.BytesIO()
    table = pa.Table.from_pandas(sample, preserve_index=False)
    pq.write_table(table, buffer)

    bytes_per_row = buffer.tell() / sample_size
    rows_per_file = max(1, int(TARGET_SIZE / bytes_per_row))

    
    for start in range(0, len(day_df), rows_per_file):
        chunk = day_df.iloc[start:start + rows_per_file]

        output_path = partition_path / f"part-{part_number:03d}.parquet"

        chunk_table = pa.Table.from_pandas(chunk, preserve_index=False)
        pq.write_table(chunk_table, output_path)

        print(f"Wrote {output_path}")
        part_number += 1

def ingest(input_path, dataset):
    print(f"Reading {input_path}")
    df = pd.read_parquet(input_path)

    print(f"Rows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")
    df["partition_date"] = df["tpep_pickup_datetime"].dt.date
    for partition_date, day_df in df.groupby("partition_date"):
        partition_path = (
           Path("data_lake")
           / dataset
           / f"year={partition_date.year}"
           / f"month={partition_date.month:02d}"
           / f"day={partition_date.day:02d}"
        )

        partition_path.mkdir(parents=True, exist_ok=True)

        day_df = day_df.drop(columns=["partition_date"])
        write_partition(day_df, partition_path)


def compact(dataset, year, month):
    month_path = (
        Path("data_lake")
        / dataset
        / f"year={year}"
        / f"month={month:02d}"
    )

    day_partitions = sorted(month_path.glob("day=*"))

    for day_path in day_partitions:
        print(f"Compacting {day_path}")

        part_files = sorted(day_path.glob("part-*.parquet"))

        if len(part_files) <= 1:
            continue

        print(f"  Found {len(part_files)} files")

        tables = [pq.read_table(file) for file in part_files]
        combined_table = pa.concat_tables(tables)
        combined_df = combined_table.to_pandas()

        for file in part_files:
            file.unlink()

        write_partition(combined_df, day_path)

def main():
    parser = argparse.ArgumentParser(
        description="Organize Parquet data into a partitioned data lake."
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_parser = subparsers.add_parser("ingest")
    ingest_parser.add_argument("input")
    ingest_parser.add_argument("--dataset", required=True)

    compact_parser = subparsers.add_parser("compact")
    compact_parser.add_argument("dataset")
    compact_parser.add_argument("--year", type=int, required=True)
    compact_parser.add_argument("--month", type=int, required=True)

    args = parser.parse_args()

    if args.command == "ingest":
        ingest(args.input, args.dataset)
    elif args.command == "compact":
        compact(args.dataset, args.year, args.month)

if __name__ == "__main__":
    main()
