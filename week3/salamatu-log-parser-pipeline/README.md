# NASA Log Parser Pipeline

## Project Overview

This project parses NASA HTTP server logs from July 1995 and converts semi-structured log data into a structured Parquet dataset for analysis.

The pipeline:

- Reads raw web server logs
- Extracts fields using Regular Expressions (Regex)
- Handles malformed records safely
- Converts fields into proper data types
- Stores clean records in Parquet format
- Stores failed records separately for investigation

---

## Dataset

Source: NASA HTTP Server Logs (July 1995)

Example log line:

199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245

Extracted fields:

| Column | Type |
|----------|----------|
| ip | string |
| timestamp | UTC datetime |
| method | string |
| path | string |
| status | integer |
| bytes | integer (nullable) |

---

## Regex Used

I used regex with named capture groups because the log data is stored as plain text and needed to be split into separate fields. Named capture groups made the extracted values easier to identify and convert into structured columns.

```python
LOG_PATTERN = re.compile(
    r'(?P<ip>\S+) '
    r'\S+ \S+ '
    r'\[(?P<timestamp>[^\]]+)\] '
    r'"(?P<method>\S+) (?P<path>\S+) \S+" '
    r'(?P<status>\d{3}) '
    r'(?P<bytes>\S+)'
)
```
### Regex Explanation

(?P<ip>\S+)
captures the client IP address or hostname.
\S+ \S+
skips the unused identity and user fields.
\[(?P<timestamp>[^\]]+)\]
captures everything inside the timestamp brackets.
(?P<method>\S+)
captures the HTTP method (GET, POST, etc.).
(?P<path>\S+)
captures the requested URL path.
(?P<status>\d{3})
captures the three-digit HTTP status code.
(?P<bytes>\S+)
captures the response size. A value of - is treated as null.

### Error Handling

If a log line cannot be parsed:
The line is not discarded, the error is written to errors.txt
The line number and failure reason are also recorded

### Output Files

Main Pipeline
clean.parquet - Parsed and validated records
errors.txt - Failed records with error details
Analysis Output - top_ips.parquet, Top 10 IP addresses by request count, top_paths.parquet, Top 10 requested resources, hourly_volume.parquet, Request volume aggregated by hour

### Schema-on-Read vs Schema-on-Write

Schema-on-Write requires you to define a fixed table structure before any data can be inserted. Schema-on-Read allows you to ingest completely raw, unstructured data immediately and apply a structure only when querying or analyzing it.

In this project, the raw NASA HTTP log file is an example of schema-on-read because it contains unstructured text records. After parsing the logs, converting fields to appropriate data types, and storing the results in a Parquet file, the dataset becomes schema-on-write.


### Results

Pipeline successfully parsed over 99% of log records while isolating malformed records into a separate error file.

### Interesting Observation

One interesting observation from the dataset is that image files accounted for many of the most frequently requested resources. /images/NASA-logosmall.gif and /images/KSC-logosmall.gif were the two most requested files, showing that a significant portion of web traffic came from browsers automatically downloading page assets rather than users directly requesting HTML pages.
