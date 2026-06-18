# NASA Log Parser

## What it does
Parses NASA HTTP server logs from 1995, extracts structured data 
and writes clean records to Parquet format.

## Regex used
(\S+) \S+ \S+ \[(.+?)\] "(\S+) (\S+) \S+" (\d{3}) (\S+)

- \S+ matches any non-whitespace (IP, method, path, status, bytes)
- .+? matches the timestamp inside brackets
- \d{3} matches exactly 3 digits for HTTP status code

## Schema on Read vs Schema on Write
Schema on write means you define the structure of your data 
before writing it — like we did with Parquet, where we explicitly 
defined column types (timestamp, int, string). The data must 
match the schema when written.

Schema on read means you define the structure when you read 
the data — like CSV, where you just store raw text and figure 
out the types later when loading it.

Parquet uses schema on write which makes reads faster because 
the types are already known.

## Results
- Total lines: 1,891,715
- Parsed: 1,886,838 (99.74%)
- Failed: 4,877 (0.26%)
- Output: 20MB Parquet from ~1GB of logs
