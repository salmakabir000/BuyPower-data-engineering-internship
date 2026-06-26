# NASA Log Parser — HTTP Access Log ETL Pipeline

A Python data pipeline that parses raw NASA-format HTTP server logs, cleans the data, and outputs structured Parquet files ready for analysis. Built to handle millions of log lines efficiently with detailed error reporting and summary analytics.

---

## What it does

Raw server logs are messy — unparseable lines, inconsistent formats, missing fields. This pipeline handles all of that, extracting every valid request into a clean, typed dataset while logging every failure with the exact reason it failed.

One run produces five outputs: a clean dataset, an error report, and three pre-computed analytics files.

---

## Requirements

Python 3.8+ and two libraries:

```bash
pip install pandas pyarrow
```

---

## Getting a log file

This pipeline is built for the **NASA HTTP access log format**. You can download the classic public dataset used in data engineering courses:

```bash
# Download and decompress the NASA July 1995 log (~1.9 million requests)
curl -O ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
gunzip NASA_access_log_Jul95.gz
```

Or use any log file in the standard Combined/Common Log Format — the kind Apache and Nginx produce by default.

A log line looks like this:

```
199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
```

---

## Usage

```bash
python parse_logs.py <input_log_file> <output_directory>
```

**Examples:**

```bash
# Parse the NASA dataset and write results to ./output
python parse_logs.py NASA_access_log_Jul95 output/

# Use a custom log file and output folder
python parse_logs.py /var/log/access.log results/
```

---

## Output files

All files are written to the directory you specify. It is created automatically if it doesn't exist.

| File | Format | Description |
|---|---|---|
| `clean.parquet` | Parquet | Every successfully parsed log line, fully typed |
| `errors.txt` | Plain text | Every failed line with its line number and failure reason |
| `top_ips.parquet` | Parquet | Top 10 IP addresses by request count |
| `top_paths.parquet` | Parquet | Top 10 most-requested URL paths |
| `hourly_volume.parquet` | Parquet | Request counts grouped by hour |

### Schema of `clean.parquet`

| Column | Type | Example |
|---|---|---|
| `ip` | string | `199.72.81.55` |
| `timestamp` | datetime (UTC) | `1995-07-01 00:00:01+00:00` |
| `method` | string | `GET` |
| `path` | string | `/history/apollo/` |
| `status` | int64 | `200` |
| `bytes` | Int64 (nullable) | `6245` |

`bytes` is nullable — a `-` in the original log is stored as `None` rather than zero, which is the correct distinction between "no bytes transferred" and "transfer size unknown."

---

## Terminal output

After every run the script prints a summary:

```
--- Pipeline Stats ---
Total lines:  1891715
Parsed:       1891714 (99.99%)
Failed:       1 (0.00%)
```

---

## Reading the output in Python

```python
import pandas as pd

# Full clean dataset
df = pd.read_parquet("output/clean.parquet")
print(df.head())

# Top requested paths
top_paths = pd.read_parquet("output/top_paths.parquet")
print(top_paths)

# Filter for server errors only
errors = df[df["status"] >= 500]
print(f"Server errors: {len(errors)}")

# Most active hour
hourly = pd.read_parquet("output/hourly_volume.parquet")
print(hourly.sort_values("requests", ascending=False).head(1))
```

---

## Example analysis queries

**Which status codes appeared most?**
```python
df["status"].value_counts()
```

**Biggest requests by bytes transferred:**
```python
df.nlargest(10, "bytes")[["path", "bytes", "status"]]
```

**Requests per day:**
```python
df.resample("D", on="timestamp").size()
```

---

## How it handles errors

Lines that don't match the expected format are not silently dropped — they are written to `errors.txt` with the line number and the exact reason for failure. This makes it easy to audit data quality or adapt the parser to a slightly different log format.

Example entry in `errors.txt`:
```
Line 1042: [malformed line content here]
Reason: Regex did not match
```

---