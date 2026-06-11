# CSV to Parquet Converter (csv2parquet)

## What is Columnar Storage?
CSV stores data row by row, like reading a book line by line.
Parquet stores data column by column, meaning if you only need 
one column from millions of rows, it only reads that column 
instead of the entire file. This makes it much faster for 
data analysis and takes up less storage space.

## What this tool does
A CLI tool that converts CSV files to Parquet format with 
different compression options.

## How to run
python3 csv2parquet.py input.csv output.parquet --compression zstd

## Compression options
- snappy (default) - fast, decent compression
- zstd - best balance of size and speed
- gzip - smallest size but slowest
- none - no compression, still smaller than CSV

## Results on NYC Taxi Dataset (296 MB CSV)
| Compression | Output Size | Write Time |
|-------------|-------------|------------|
| zstd        | 41 MB       | 1.7s       |
| snappy      | 54 MB       | 1.9s       |
| gzip        | 41 MB       | 44.8s      |
| none        | 74 MB       | 1.4s       |
