import argparse
import json
import math
from pathlib import Path

import pandas as pd


DEFAULT_OUTPUT_DIR = Path("data_lake")
TARGET_MIN_MB = 64
TARGET_MAX_MB = 128
TARGET_BYTES = 128 * 1024 * 1024


def get_partition_columns(df):
    """Create Hive-style partition columns from pickup datetime."""
    if "tpep_pickup_datetime" not in df.columns:
        raise ValueError(
            "Input file must contain 'tpep_pickup_datetime' column."
        )

    pickup = pd.to_datetime(df["tpep_pickup_datetime"], errors="coerce")

    if pickup.isna().any():
        bad_rows = int(pickup.isna().sum())
        raise ValueError(
            f"Found {bad_rows} rows with invalid tpep_pickup_datetime values."
        )

    df = df.copy()
    df["year"] = pickup.dt.year
    df["month"] = pickup.dt.month
    df["day"] = pickup.dt.day

    return df


def partition_path(output_dir, dataset, year, month, day):
    """Return the Hive-style partition directory."""
    return (
        Path(output_dir)
        / dataset
        / f"year={year:04d}"
        / f"month={month:02d}"
        / f"day={day:02d}"
    )


def write_dataframe_chunks(df, partition_dir):
    """
    Write a partition into Parquet files.
    Files are split by estimated size so they stay around 64–128 MB.
    """
    partition_dir.mkdir(parents=True, exist_ok=True)

    if df.empty:
        return []

    # Estimate bytes per row using a sample.
    sample_size = min(len(df), 10_000)
    sample = df.head(sample_size)

    estimated_sample_bytes = len(
        sample.to_parquet(index=False)
    )

    bytes_per_row = max(
        estimated_sample_bytes / max(len(sample), 1),
        1,
    )

    rows_per_file = max(
        1,
        int(TARGET_BYTES / bytes_per_row),
    )

    files_created = []

    for start in range(0, len(df), rows_per_file):
        chunk = df.iloc[start : start + rows_per_file]

        part_number = len(files_created) + 1
        output_file = partition_dir / f"part-{part_number:03d}.parquet"

        chunk.to_parquet(
            output_file,
            engine="pyarrow",
            index=False,
        )

        files_created.append(output_file)

    return files_created


def ingest(input_file, dataset, output_dir):
    """Ingest one Parquet file into Hive-style partitions."""
    print(f"Reading {input_file}...")

    input_path = Path(input_file)

    if not input_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_file}"
        )

    df = pd.read_parquet(input_path)

    print(f"Rows read: {len(df):,}")

    df = get_partition_columns(df)

    created_files = []

    grouped = df.groupby(
        ["year", "month", "day"],
        sort=True,
    )

    for (year, month, day), partition_df in grouped:
        partition_df = partition_df.drop(
            columns=["year", "month", "day"]
        )

        partition_dir = partition_path(
            output_dir,
            dataset,
            year,
            month,
            day,
        )

        files = write_dataframe_chunks(
            partition_df,
            partition_dir,
        )

        created_files.extend(files)

        # Stretch goal: _SUCCESS marker.
        success_file = partition_dir / "_SUCCESS"
        success_file.touch()

        # Stretch goal: manifest.
        manifest = {
            "dataset": dataset,
            "year": int(year),
            "month": int(month),
            "day": int(day),
            "files": [file.name for file in files],
            "rows": int(len(partition_df)),
        }

        with open(
            partition_dir / "_manifest.json",
            "w",
            encoding="utf-8",
        ) as file:
            json.dump(manifest, file, indent=2)

    print(f"Partitions written: {len(created_files)}")
    print("Ingest completed successfully.")


def find_partition_files(partition_dir):
    """Find Parquet part files in one partition."""
    return sorted(
        partition_dir.glob("part-*.parquet")
    )


def compact_partition(partition_dir):
    """Compact files inside one partition."""
    files = find_partition_files(partition_dir)

    if len(files) <= 1:
        return 0, len(files)

    print(
        f"Compacting {partition_dir} "
        f"({len(files)} files)..."
    )

    frames = []

    for file in files:
        frames.append(pd.read_parquet(file))

    combined = pd.concat(
        frames,
        ignore_index=True,
    )

    for file in files:
        file.unlink()

    new_files = write_dataframe_chunks(
        combined,
        partition_dir,
    )

    success_file = partition_dir / "_SUCCESS"
    success_file.touch()

    manifest = {
        "partition": str(partition_dir),
        "files": [file.name for file in new_files],
        "rows": int(len(combined)),
    }

    with open(
        partition_dir / "_manifest.json",
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(manifest, file, indent=2)

    return len(files), len(new_files)


def compact(dataset, year, month, output_dir):
    """Compact all day partitions for a specified year/month."""
    month_dir = (
        Path(output_dir)
        / dataset
        / f"year={year:04d}"
        / f"month={month:02d}"
    )

    if not month_dir.exists():
        raise FileNotFoundError(
            f"Partition not found: {month_dir}"
        )

    total_before = 0
    total_after = 0

    day_dirs = sorted(
        path
        for path in month_dir.iterdir()
        if path.is_dir() and path.name.startswith("day=")
    )

    for day_dir in day_dirs:
        before, after = compact_partition(day_dir)

        total_before += before
        total_after += after

    print(
        f"Compaction completed. "
        f"Files before: {total_before}, "
        f"files after: {total_after}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Organize Parquet data into a Hive-style data lake."
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    ingest_parser = subparsers.add_parser(
        "ingest",
        help="Ingest a Parquet file into date partitions.",
    )

    ingest_parser.add_argument(
        "input",
        help="Input Parquet file.",
    )

    ingest_parser.add_argument(
        "--dataset",
        required=True,
        help="Dataset name, e.g. trips.",
    )

    ingest_parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Data lake output directory.",
    )

    compact_parser = subparsers.add_parser(
        "compact",
        help="Compact files in a year/month partition.",
    )

    compact_parser.add_argument(
        "dataset",
        help="Dataset name, e.g. trips.",
    )

    compact_parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Year to compact.",
    )

    compact_parser.add_argument(
        "--month",
        type=int,
        required=True,
        help="Month to compact.",
    )

    compact_parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Data lake output directory.",
    )

    args = parser.parse_args()

    if args.command == "ingest":
        ingest(
            args.input,
            args.dataset,
            args.output_dir,
        )

    elif args.command == "compact":
        compact(
            args.dataset,
            args.year,
            args.month,
            args.output_dir,
        )


if __name__ == "__main__":
    main()