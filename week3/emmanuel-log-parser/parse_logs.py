import argparse
import re
import pandas as pd

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_file")
    parser.add_argument("output_dir")

    args = parser.parse_args()

    pattern = r'^(\S+) .*?"(\S+) (\S+) .*?" (\d{3}) (\S+)'

    total_lines = 0
    parsed_lines = 0
    bad_lines = 0
    good_records = []

    error_file = open("errors.txt", "w", encoding="utf-8")

    with open(args.input_file, "r", encoding="latin-1") as file:
        for line in file:
            total_lines += 1

            match = re.match(pattern, line)

            if match:
                parsed_lines += 1

                ip = match.group(1)
                method = match.group(2)
                path = match.group(3)
                status = int(match.group(4))
                bytes_sent = match.group(5)

                good_records.append(
                    [ip, method, path, status, bytes_sent]
                )

            else:
                bad_lines += 1
                error_file.write(line)

    error_file.close()

    df = pd.DataFrame(
        good_records,
        columns=["ip", "method", "path", "status", "bytes"]
    )

    df.to_parquet("clean.parquet", index=False)

    print("Total lines:", total_lines)
    print("Parsed lines:", parsed_lines)
    print("Bad lines:", bad_lines)

if __name__ == "__main__":
    main()