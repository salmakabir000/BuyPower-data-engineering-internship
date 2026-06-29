import argparse
import os
import time
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("input_csv")
parser.add_argument("output_parquet")
parser.add_argument(
    "--compression",
    default="snappy",
    choices=["snappy", "gzip", "zstd", "none"]
)

args = parser.parse_args()

compression = None if args.compression == "none" else args.compression

# Check input file size
input_size = os.path.getsize(args.input_csv)

# Read CSV (use chunks if larger than 500 MB)
start_csv_read = time.time()

if input_size > 500 * 1024 * 1024:
    chunks = []
    for chunk in pd.read_csv(args.input_csv, chunksize=100000):
        chunks.append(chunk)
    df = pd.concat(chunks, ignore_index=True)
else:
    df = pd.read_csv(args.input_csv)

end_csv_read = time.time()

# Write Parquet
start_write = time.time()

df.to_parquet(
    args.output_parquet,
    engine="pyarrow",
    compression=compression
)

end_write = time.time()

# Read Parquet back
start_parquet_read = time.time()
pd.read_parquet(args.output_parquet)
end_parquet_read = time.time()

# Calculate output size and compression ratio
output_size = os.path.getsize(args.output_parquet)
compression_ratio = (
    input_size / output_size if output_size > 0 else 0
)

print("\n===== SUMMARY REPORT =====")
print(f"Input size: {input_size} bytes")
print(f"Output size: {output_size} bytes")
print(f"Compression ratio: {compression_ratio:.2f}x")
print(f"Write time: {end_write - start_write:.2f} seconds")
print(f"Read CSV time: {end_csv_read - start_csv_read:.2f} seconds")
print(f"Read Parquet time: {end_parquet_read - start_parquet_read:.2f} seconds")