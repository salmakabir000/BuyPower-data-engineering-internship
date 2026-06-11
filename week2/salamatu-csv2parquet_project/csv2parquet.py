import argparse
import pandas as pd
import time
import os

parser = argparse.ArgumentParser(description="CSV to Parquet converter")

parser.add_argument("input_file", help="Path to input CSV file")
parser.add_argument("output_file", help="Path to output Parquet file")
parser.add_argument(
    "--compression",
    default="snappy",
    choices=["snappy", "zstd", "gzip", "none"],
    help="Compression type for Parquet"
)

args = parser.parse_args()

file_size_mb = os.path.getsize(args.input_file) / (1024 * 1024)
print(f"File size: {file_size_mb:.2f} MB")

start = time.time()
if file_size_mb > 500:
    print("Large file: Reading CSV in chunks")

    chunks = []

    for chunk in pd.read_csv(args.input_file, chunksize=200000, low_memory=False):
        chunks.append(chunk)
    df = pd.concat(chunks)
else:
    print("Small file: using normal reading")
    df = pd.read_csv(args.input_file, low_memory=False)

print("Writing Parquet...")

df.to_parquet(
    args.output_file,
    engine="pyarrow",
    compression = None if args.compression == "none" else args.compression,
)
end = time.time()

input_size = file_size_mb
output_size = os.path.getsize(args.output_file) / (1024 * 1024)
ratio = input_size / output_size if output_size != 0 else 0

t1 = time.time()
pd.read_csv(args.input_file, low_memory=False)
t2 = time.time()
csv_read_time = t2 - t1

t1 = time.time()
pd.read_parquet(args.output_file)
t2 = time.time()
parquet_read_time = t2 - t1

print("\n--- Summary Report ---")
print(f"Input:  {input_size:.2f} MB")
print(f"Output: {output_size:.2f} MB")
print(f"Ratio:  {ratio:.2f}x smaller")
print(f"Write:  {end - start:.2f}s")
print(f"Read CSV: {csv_read_time:.2f}s")
print(f"Read Parquet: {parquet_read_time:.2f}s")
print(f"Compression: {args.compression}")
