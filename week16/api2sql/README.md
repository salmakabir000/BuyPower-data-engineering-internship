\# REST API to SQL Bridge



\## Project Overview



This project extracts data from a REST API and loads it into a PostgreSQL database.



The pipeline is designed to support automated data extraction, pagination, incremental loading, error handling, and database upserts.



\## Features



\- REST API data extraction.

\- API authentication using a GitHub token.

\- Pagination support.

\- Automatic retries for failed requests.

\- Rate limiting.

\- Incremental data loading using saved cursors.

\- PostgreSQL database integration.

\- Insert and update records using upserts.

\- Pipeline status monitoring.

\- Configuration validation.



\## Project Structure



```text

api2sql/

│

├── api2sql.py

├── requirements.txt

├── README.md

│

└── configs/

&#x20;   └── github\_issues.yml

