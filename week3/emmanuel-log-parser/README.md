# Log Parser Pipeline

## What this script does

This script reads the NASA web server log file and parses each line using a regular expression.

For every valid line it extracts:

* IP address
* HTTP method
* Path
* Status code
* Bytes sent

Valid records are written to `clean.parquet`.

Invalid records are written to `errors.txt` so that bad data is not lost.

## Results

* Total lines: 1,891,715
* Parsed lines: 1,888,786
* Bad lines: 2,929

This means more than 99% of the records were parsed successfully.

## Regex used

```python
r'^(\S+) .*?"(\S+) (\S+) .*?" (\d{3}) (\S+)'
```

### Why I used this regex

The NASA log file follows a similar pattern on most lines.

The regex captures:

1. IP address
2. HTTP method
3. Path
4. Status code
5. Bytes sent

These are the fields required for the assignment.

## Schema on Read vs Schema on Write

Schema on Read means the structure is applied when the data is read.

Schema on Write means the structure is enforced before the data is stored.

In this assignment I converted the parsed log data into typed columns before saving it as a Parquet file, which is closer to Schema on Write.

## Files produced

* clean.parquet
* errors.txt
* parse_logs.py
