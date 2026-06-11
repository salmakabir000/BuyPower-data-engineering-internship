import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pa_csv
import time
import os
import click

@click.command()
@click.argument('input_csv')
@click.argument('output_parquet')
@click.option('--compression', default='snappy', help='Compression: snappy, zstd, gzip, none')
def csv2parquet(input_csv, output_parquet, compression):
    """Convert a CSV file to Parquet format."""
    
    input_size = os.path.getsize(input_csv) / (1024 * 1024)
    
    print(f"Reading CSV...")
    start = time.time()
    
    if input_size > 500:
        chunks = []
        for chunk in pd.read_csv(input_csv, chunksize=100000):
            chunks.append(pa.Table.from_pandas(chunk))
        table = pa.concat_tables(chunks)
    else:
        table = pa_csv.read_csv(input_csv)
    
    comp = None if compression == 'none' else compression
    
    print(f"Writing Parquet...")
    write_start = time.time()
    pq.write_table(table, output_parquet, compression=comp)
    write_time = time.time() - write_start
    
    output_size = os.path.getsize(output_parquet) / (1024 * 1024)
    ratio = input_size / output_size
    
    print(f"\nInput:  {input_size:.0f} MB CSV")
    print(f"Output: {output_size:.0f} MB Parquet ({compression})")
    print(f"Ratio:  {ratio:.0f}x smaller")
    print(f"Write:  {write_time:.1f}s")
    
    print(f"\nReading back...")
    csv_start = time.time()
    pd.read_csv(input_csv, nrows=100000)
    csv_read = time.time() - csv_start
    
    parquet_start = time.time()
    pd.read_parquet(output_parquet)
    parquet_read = time.time() - parquet_start
    
    print(f"Read CSV:     {csv_read:.1f}s")
    print(f"Read Parquet: {parquet_read:.1f}s")

if __name__ == '__main__':
    csv2parquet()
