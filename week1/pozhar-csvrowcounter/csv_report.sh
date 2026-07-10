#!/bin/bash

# Help message
if [ "$1" == "-h" ]; then
    echo "Usage: ./csv_report.sh [directory]"
    echo ""
    echo "Finds all .csv files in a directory, counts rows and saves a summary to report.txt"
    echo ""
    echo "Arguments:"
    echo "  directory   Path to directory (default: current directory)"
    echo ""
    echo "Options:"
    echo "  -h          Show this help message"
    exit 0
fi

DIR=${1:-.}
REPORT="report.txt"

# Validate directory exists
if [ ! -d "$DIR" ]; then
    echo "Error: Directory '$DIR' does not exist."
    exit 1
fi

echo "CSV Row Count Report" > $REPORT
echo "====================" >> $REPORT
echo "Generated: $(date)" >> $REPORT
echo "" >> $REPORT

for file in "$DIR"/*.csv; do
    if [ -f "$file" ]; then
        rows=$(wc -l < "$file")
        echo "$file: $rows rows" >> $REPORT
    fi
done

echo "Done! Report saved to $REPORT"
