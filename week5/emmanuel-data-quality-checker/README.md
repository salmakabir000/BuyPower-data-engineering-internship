\# Data Quality Checker



\## Overview



This project validates datasets using rules defined in a YAML configuration file.



It loads data from a SQLite database and performs data quality checks before the data is used downstream.



\## Supported Checks



\* not\_null

\* unique

\* range

\* regex\_match

\* row\_count\_min

\* in\_set



\## How to Run



```bash

python dq.py

```



\## Example Output



```text

Data Quality Report - coin\_prices

Total rows checked: 770



id not\_null: 0 failures

id unique: 518 duplicates

current\_price range: 0 failures

symbol regex\_match: 8 failures

row\_count\_min passed (770 >= 100)

symbol in\_set: 745 failures



Result: PASSED

```



\## Exit Codes



\* Exit code 0 = Success / warnings only

\* Exit code 1 = Critical check failure



