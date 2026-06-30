#!/bin/bash

# usage message printed when run with -h
if [ "$1" = "-h" ]
then
   echo "Usage: ./script.sh <directory>"
   echo "Example: ./script.sh data"
   exit 0
fi

# validating input to check if empty or not exist
DIR=$1

if [ -z "$DIR" ]
then
    echo "Error: no directory given"
    exit 1

elif [ ! -d "$DIR" ]
then
    echo "Error: directory does not exist"
    exit 1
fi

# setup report
mkdir -p reports
REPORT="reports/report.txt"

echo "CSV Counter Starting...."
echo "CSV Row Counting Report" > "$REPORT"

# checking if the file empty
found=0

while read file
do
    filename=$(basename "$file")
    lines=$(wc -l < "$file")
    echo "$filename has $lines rows" >> "$REPORT"
    found=1
done < <(find "$DIR" -name "*.csv")

if [ $found -eq 0 ]
then
    echo "No CSV files found in directory" >> "$REPORT"
    echo "No CSV files found."
fi

echo "Counting done."
