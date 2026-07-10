# CSV Row Counter

This project was built as part of my Linux and Shell Scripting learning journey. The goal was to automate the process of discovering CSV files and generating a row count report without requiring any manual intervention.

The script:

- Accepts a directory as a command-line argument
- Validates user input
- Handles missing or invalid directories gracefully
- Recursively finds all `.csv` files
- Counts rows in each file using Linux command-line tools
- Generates a report file automatically
- Provides a help message using the `-h` flag

---

## Project Structure

```text
csv_row_counter/
├── data/
│   ├── Dim_Customers.csv
│   ├── Dim_Location.csv
│   ├── Dim_Products.csv
│   ├── Dim_Shipping.csv
│   └── Fact_Sales.csv
├── reports/
│   └── report.txt
├── script.sh
└── README.md
```

---

## Technologies Used

- Bash
- Linux Terminal
- Git & GitHub

### Linux Commands and Concepts Used

- `find`
- `wc`
- `mkdir`
- `basename`
- Variables
- Conditional statements (`if`, `elif`)
- Loops (`while read`)
- Command substitution (`$(...)`)
- Process substitution (`< <(...)`)
- Input and output redirection (`<`, `>`, `>>`)
- Exit codes

---

## How to Run

### Make the script executable

```bash
chmod +x script.sh
```

### Run the script

```bash
./script.sh data
```

### Display help information

```bash
./script.sh -h
```

---

## Example Output

Terminal:

```text
CSV Counter Starting....
Counting done.
```

Generated Report (`reports/report.txt`):

```text
CSV Row Counting Report
Dim_Products.csv has 1863 rows
Dim_Customers.csv has 794 rows
Dim_Shipping.csv has 17 rows
Dim_Location.csv has 632 rows
Fact_Sales.csv has 9995 rows
```

---

## Error Handling

### Missing directory argument

```bash
./script.sh
```

Output:

```text
Error: no directory given
```

### Directory does not exist

```bash
./script.sh test
```

Output:

```text
Error: directory does not exist
```

### Help menu

```bash
./script.sh -h
```

Output:

```text
Usage: ./script.sh <directory>
Example: ./script.sh data
```

---


Through this project, I gained hands-on experience with:

- Linux file system navigation
- Shell scripting fundamentals
- Bash variables and command-line arguments
- File discovery using `find`
- Row counting with `wc -l`
- Redirection and process substitution
- Error handling and exit codes
- Git branching and pull request workflows

---
