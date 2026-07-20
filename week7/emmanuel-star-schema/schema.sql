CREATE TABLE dim_actor (
    actor_sk SERIAL PRIMARY KEY,
    actor_id BIGINT UNIQUE,
    login TEXT,
    display_name TEXT,
    url TEXT
);

CREATE TABLE dim_repo (
    repo_sk SERIAL PRIMARY KEY,
    repo_id BIGINT UNIQUE,
    repo_name TEXT,
    repo_url TEXT
);

CREATE TABLE dim_event_type (
    event_type_sk SERIAL PRIMARY KEY,
    event_type_name TEXT UNIQUE
);

CREATE TABLE dim_date (
    date_sk INTEGER PRIMARY KEY,
    full_date DATE,
    year INTEGER,
    month INTEGER,
    day INTEGER
);

CREATE TABLE fact_events (
    event_sk SERIAL PRIMARY KEY,
    event_id TEXT UNIQUE,
    event_type_sk INTEGER REFERENCES dim_event_type(event_type_sk),
    actor_sk INTEGER REFERENCES dim_actor(actor_sk),
    repo_sk INTEGER REFERENCES dim_repo(repo_sk),
    date_sk INTEGER REFERENCES dim_date(date_sk),
    created_at TIMESTAMP
);