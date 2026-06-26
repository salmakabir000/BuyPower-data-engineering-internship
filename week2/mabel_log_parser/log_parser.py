import re
import sys
import pandas as pd
from datetime import datetime
from collections import Counter
import os

# Regex pattern for NASA logs
LOG_PATTERN = re.compile(
    r'^(\S+) \S+ \S+ \[([^\]]+)\] "([^"]+)" (\d{3}) (\S+)$'
)

# Parse timestamp
def parse_timestamp(ts):
    return datetime.strptime(ts, "%d/%b/%Y:%H:%M:%S %z")

# Parse request field
def parse_request(request):
    parts = request.split()
    if len(parts) != 3:
        raise ValueError("Invalid request format")
    method, path, _ = parts
    return method, path

# Main parser
def parse_logs(input_file, output_dir):
    clean_data = []
    errors = []

    ip_counter = Counter()
    path_counter = Counter()
    hourly_counter = Counter()

    total = 0
    parsed = 0

    os.makedirs(output_dir, exist_ok=True)

    with open(input_file, "r", encoding="utf-8", errors="ignore") as f:
        for line_no, line in enumerate(f, 1):
            total += 1
            line = line.strip()

            try:
                match = LOG_PATTERN.match(line)

                if not match:
                    raise ValueError("Regex did not match")

                ip, timestamp, request, status, bytes_ = match.groups()

                # Convert fields
                dt = parse_timestamp(timestamp)
                method, path = parse_request(request)
                status = int(status)
                bytes_ = None if bytes_ == "-" else int(bytes_)

                # Store clean record
                record = {
                    "ip": ip,
                    "timestamp": dt,
                    "method": method,
                    "path": path,
                    "status": status,
                    "bytes": bytes_
                }

                clean_data.append(record)

                # Stats
                ip_counter[ip] += 1
                path_counter[path] += 1
                hourly_counter[dt.strftime("%Y-%m-%d %H:00:00")] += 1

                parsed += 1

            except Exception as e:
                errors.append(f"Line {line_no}: {line}\nReason: {str(e)}\n")


    # Create DataFrame

    df = pd.DataFrame(clean_data)

    # Ensure correct types
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["status"] = df["status"].astype("int64")
    df["bytes"] = df["bytes"].astype("float").astype("Int64")  # allows None

    #
    # Write outputs
    df.to_parquet(os.path.join(output_dir, "clean.parquet"), index=False)

    with open(os.path.join(output_dir, "errors.txt"), "w") as f:
        f.writelines(errors)


    # Stretch analytics
    
    top_ips = pd.DataFrame(ip_counter.most_common(10), columns=["ip", "requests"])
    top_paths = pd.DataFrame(path_counter.most_common(10), columns=["path", "requests"])
    hourly = pd.DataFrame(hourly_counter.items(), columns=["hour", "requests"])

    top_ips.to_parquet(os.path.join(output_dir, "top_ips.parquet"), index=False)
    top_paths.to_parquet(os.path.join(output_dir, "top_paths.parquet"), index=False)
    hourly.to_parquet(os.path.join(output_dir, "hourly_volume.parquet"), index=False)

    
    failed = total - parsed

    print("\n--- Pipeline Stats ---")
    print(f"Total lines: {total}")
    print(f"Parsed: {parsed} ({parsed/total*100:.2f}%)")
    print(f"Failed: {failed} ({failed/total*100:.2f}%)")

# CLI entry point
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python parse_logs.py <input_file> <output_dir>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_dir = sys.argv[2]

    parse_logs(input_file, output_dir)