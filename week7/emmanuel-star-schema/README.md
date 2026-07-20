\# Week 7 - GitHub Archive Star Schema



\## Project Overview



This project loads GitHub Archive event data into a PostgreSQL star schema.



The data is extracted from a compressed GitHub Archive JSON file, transformed into dimension and fact records, and loaded into PostgreSQL.



\## Star Schema



\### Dimension Tables



\- `dim\_actor` - Stores GitHub actors/users.

\- `dim\_repo` - Stores GitHub repositories.

\- `dim\_event\_type` - Stores different GitHub event types.

\- `dim\_date` - Stores calendar date information.



\### Fact Table



\- `fact\_events` - Stores GitHub events and links to the dimension tables using surrogate keys.



\## Data Source



GitHub Archive data was downloaded as a compressed JSON file:



`2024-01-15-12.json.gz`



The Python loader reads the compressed JSON file and loads the data into PostgreSQL.



\## Technologies Used



\- Python

\- PostgreSQL

\- Docker

\- psycopg2

\- GitHub Archive JSON data



\## ETL Process



1\. Read the compressed GitHub Archive JSON file.

2\. Extract event type, actor, and repository information.

3\. Insert unique records into the dimension tables.

4\. Retrieve surrogate keys from the dimension tables.

5\. Insert event records into the `fact\_events` fact table.

6\. Use SQL queries to analyze the loaded data.



\## Validation



The pipeline successfully loaded 100 GitHub events into the fact table.



Example validation query:



```sql

SELECT COUNT(\*) FROM fact\_events;

