import re
import sys
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd


LOG_PATTERN = re.compile(
    r'(?P<ip>\S+) '
    r'\S+ \S+ '
    r'\[(?P<timestamp>[^\]]+)\] '
    r'"(?P<method>\S+) (?P<path>\S+) \S+" '
    r'(?P<status>\d{3}) '
    r'(?P<bytes>\S+)'
)


def parse_line(line):
    """
    Parse one log line.
    Returns dictionary of fields.
    """

    match = LOG_PATTERN.match(line)

    if not match:
        raise ValueError("Regex match failed")

    data = match.groupdict()

    timestamp = datetime.strptime(
        data["timestamp"],
        "%d/%b/%Y:%H:%M:%S %z"
    )

    timestamp = timestamp.astimezone(timezone.utc)

    bytes_value = (
        None
        if data["bytes"] == "-"
        else int(data["bytes"])
    )

    return {
        "ip": data["ip"],
        "timestamp": timestamp,
        "method": data["method"],
        "path": data["path"],
        "status": int(data["status"]),
        "bytes": bytes_value,
    }


def main():

    if len(sys.argv) != 3:
        print(
            "Usage: python parse_logs.py "
            "<input_file> <output_dir>"
        )
        sys.exit(1)

    input_file = Path(sys.argv[1])
    output_dir = Path(sys.argv[2])

    output_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    records = []
    errors = []

    total_lines = 0
    parsed_lines = 0
    failed_lines = 0

    with open(input_file, "r", encoding="latin-1") as f:

        for line_number, line in enumerate(
            f,
            start=1
        ):

            total_lines += 1

            line = line.strip()

            try:

                record = parse_line(line)

                records.append(record)

                parsed_lines += 1

            except Exception as e:

                failed_lines += 1

                errors.append(
                    f"Line {line_number}: {e}\n"
                    f"{line}\n"
                )

    df = pd.DataFrame(records)

    df.to_parquet(
        output_dir / "clean.parquet",
        index=False
    )

    with open(
        output_dir / "errors.txt",
        "w"
    ) as f:

        f.writelines(errors)

    print("\nPipeline Finished\n")

    print(
        f"Total lines: "
        f"{total_lines:,}"
    )

    print(
        f"Parsed: "
        f"{parsed_lines:,} "
        f"({parsed_lines/total_lines*100:.2f}%)"
    )

    print(
        f"Failed: "
        f"{failed_lines:,} "
        f"({failed_lines/total_lines*100:.2f}%)"
    )


if __name__ == "__main__":
    main()
