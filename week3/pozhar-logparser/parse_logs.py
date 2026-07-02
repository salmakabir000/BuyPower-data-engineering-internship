import re
import pyarrow as pa
import pyarrow.parquet as pq
import sys
import os
from datetime import datetime, timezone

# Regex pattern to parse each log line
LOG_PATTERN = re.compile(
    r'(\S+) \S+ \S+ \[(.+?)\] "(\S+) (\S+) \S+" (\d{3}) (\S+)'
)

def parse_timestamp(ts):
    return datetime.strptime(ts, "%d/%b/%Y:%H:%M:%S %z").astimezone(timezone.utc)

def parse_bytes(b):
    return None if b == '-' else int(b)

def parse_logs(input_file, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    good_records = []
    bad_lines = []
    total = 0

    with open(input_file, 'r', errors='replace') as f:
        for line_num, line in enumerate(f, 1):
            total += 1
            line = line.strip()
            match = LOG_PATTERN.match(line)
            if match:
                try:
                    ip, ts, method, path, status, bytes_ = match.groups()
                    good_records.append({
                        'ip': ip,
                        'timestamp': parse_timestamp(ts),
                        'method': method,
                        'path': path,
                        'status': int(status),
                        'bytes': parse_bytes(bytes_)
                    })
                except Exception as e:
                    bad_lines.append(f"Line {line_num}: {e} | {line}\n")
            else:
                bad_lines.append(f"Line {line_num}: no match | {line}\n")

    # Write clean parquet
    table = pa.table({
        'ip': pa.array([r['ip'] for r in good_records], type=pa.string()),
        'timestamp': pa.array([r['timestamp'] for r in good_records], type=pa.timestamp('us', tz='UTC')),
        'method': pa.array([r['method'] for r in good_records], type=pa.string()),
        'path': pa.array([r['path'] for r in good_records], type=pa.string()),
        'status': pa.array([r['status'] for r in good_records], type=pa.int32()),
        'bytes': pa.array([r['bytes'] for r in good_records], type=pa.float64()),
    })
    pq.write_table(table, os.path.join(output_dir, 'clean.parquet'))

    # Write errors
    with open(os.path.join(output_dir, 'errors.txt'), 'w') as ef:
        ef.writelines(bad_lines)

    # Print stats
    parsed = len(good_records)
    failed = len(bad_lines)
    print(f"Total lines:  {total:,}")
    print(f"Parsed:       {parsed:,} ({parsed/total*100:.2f}%)")
    print(f"Failed:       {failed:,} ({failed/total*100:.2f}%)")
    print(f"Output:       {os.path.join(output_dir, 'clean.parquet')}")

if __name__ == '__main__':
    parse_logs(sys.argv[1], sys.argv[2])
