#!/bin/bash
DIR=${1:-.}
REPORT="report.txt"

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
