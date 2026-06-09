# CSV Row Counter

## What it does
This script finds all .csv files in a directory, counts the rows in each and writes a summary to a report.txt file.

## How to run
chmod +x csv_report.sh
./csv_report.sh

## How it works
- Loops through all .csv files in the current directory
- Uses wc -l to count rows in each file
- Writes results to report.txt with a timestamp
