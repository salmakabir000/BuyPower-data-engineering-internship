# CSV to Parquet Converter (CLI Tool)

## Overview

This project is a simple command-line tool that converts CSV files into Parquet format and compares both formats in terms of size and speed.

The goal was to understand how data storage differs between **row-based formats (CSV)** and **column-based formats (Parquet)**, and how that affects performance and storage.

---

## What I Learned

### 1. CSV vs Parquet (Row vs Column Storage)

CSV stores data row by row, which is simple but not efficient for large datasets.

Parquet stores data by columns, which makes it smaller in size, faster to read for analytics, more efficient because similar values are grouped together

---

### 2. Compression

I tested different compression methods:

- **Snappy** → balanced speed and compression
- **ZSTD** → better compression with good performance
- **Gzip** → smallest file size but slowest write speed
- **None** → no compression

Main lesson: smaller file size is not always better if it slows down processing too much.

---

### 3. Chunking for Large Files

For large CSV files (>500MB), the script reads data in chunks instead of loading everything into memory at once.
This helps prevent memory issues when working with big datasets.

---

## Features

- CLI tool using `argparse`
- Converts CSV → Parquet
- Supports multiple compression options
- Checks file size before processing
- Uses chunking for large files
- Measures performance:Write time, CSV read time, Parquet read time
- Prints a summary report after execution

---

## How to Run

```bash
python csv2parquet.py input.csv output.parquet --compression zstd
```
## Compression Options:
- snappy (default)
- zstd
- gzip
- none

## Main Insight

Parquet is not just smaller because of compression.
It is smaller and faster mainly because it uses columnar storage instead of row-based storage, which makes both compression and reading more efficient.

## Tools Used
- Python
- Pandas
- PyArrow
- argparse

## What This Project Taught Me

This project helped me understand how real data systems optimize storage and performance using:
- file formats (CSV vs Parquet)
- compression tradeoffs
- memory-efficient processing using chunking
