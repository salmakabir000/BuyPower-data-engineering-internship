import argparse
import pandas as pd
import os
import time
import pyarrow as pa
import pyarrow.parquet as pq


def get_directory_size(path):
    total_size = 0

    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            filepath = os.path.join(
                dirpath,
                filename
            )

            total_size += os.path.getsize(
                filepath
            )

    return total_size


parser = argparse.ArgumentParser(
    description="Convert CSV to Parquet"
)

parser.add_argument(
    "input_csv",
    help="Path to input CSV file"
)

parser.add_argument(
    "output_parquet",
    help="Output Parquet file or dataset folder"
)

parser.add_argument(
    "--compression",
    default="snappy",
    choices=["snappy", "zstd", "gzip", "none"],
    help="Parquet compression type"
)

parser.add_argument(
    "--partition-by",
    choices=["month"],
    default=None,
    help="Partition dataset by month"
)

args = parser.parse_args()

input_size = os.path.getsize(
    args.input_csv
)

input_mb = input_size / (1024 * 1024)

compression = (
    None
    if args.compression == "none"
    else args.compression
)

use_chunks = (
    input_mb > 500
    and args.partition_by is None
)


write_start = time.time()

if use_chunks:

    first_chunk = True

    for chunk in pd.read_csv(
        args.input_csv,
        chunksize=100000
    ):

        table = pa.Table.from_pandas(
            chunk
        )

        if first_chunk:

            writer = pq.ParquetWriter(
                args.output_parquet,
                table.schema,
                compression=compression
            )

            first_chunk = False

        writer.write_table(
            table
        )

    writer.close()

else:

    df = pd.read_csv(
        args.input_csv
    )

    if args.partition_by == "month":

        df["tpep_pickup_datetime"] = pd.to_datetime(
            df["tpep_pickup_datetime"]
        )

        df["month"] = (
            df["tpep_pickup_datetime"]
            .dt.month
        )

        table = pa.Table.from_pandas(
            df
        )

        pq.write_to_dataset(
            table,
            root_path=args.output_parquet,
            partition_cols=["month"]
        )

    else:

        df.to_parquet(
            args.output_parquet,
            compression=compression,
            index=False
        )

write_time = (
    time.time() - write_start
)


if args.partition_by == "month":

    output_size = get_directory_size(
        args.output_parquet
    )

else:

    output_size = os.path.getsize(
        args.output_parquet
    )

output_mb = (
    output_size / (1024 * 1024)
)

ratio = (
    input_size / output_size
)


start = time.time()

pd.read_csv(
    args.input_csv
)

csv_read_time = (
    time.time() - start
)


start = time.time()

pd.read_parquet(
    args.output_parquet
)

parquet_read_time = (
    time.time() - start
)


print(
    f"Input: {input_mb:.1f} MB CSV"
)

print(
    f"Output: {output_mb:.1f} MB Parquet ({args.compression})"
)

print(
    f"Ratio: {ratio:.1f}x smaller"
)

print(
    f"Write: {write_time:.2f}s"
)

print(
    f"Read CSV: {csv_read_time:.2f}s"
)

print(
    f"Read Parquet: {parquet_read_time:.2f}s"
)


if args.partition_by == "month":

    start = time.time()

    full_df = pd.read_parquet(
        args.output_parquet
    )

    january_rows = full_df[
        full_df["month"] == 1
    ]

    single_file_filter_time = (
        time.time() - start
    )

    partition_path = (
        f"{args.output_parquet}/month=1"
    )

    start = time.time()

    january_partition = pd.read_parquet(
        partition_path
    )

    partition_read_time = (
        time.time() - start
    )

    print(
        f"Filter Month From Dataset: "
        f"{single_file_filter_time:.2f}s"
    )

    print(
        f"Read Month Partition: "
        f"{partition_read_time:.2f}s"
    )


#python csv2parquet.py \yellow_tripdata_2024-01.csv \output.parquet                                 to run normally
#python csv2parquet.py \yellow_tripdata_2024-01.csv \output.parquet \--compression zstd             to run with compressions
#python csv2parquet.py \yellow_tripdata_2024-01.csv \partitioned_dataset --partition-by month       to run partition by month