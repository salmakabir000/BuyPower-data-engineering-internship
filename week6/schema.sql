
CREATE TABLE dim_actor (
    actor_sk INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    actor_id BIGINT UNIQUE NOT NULL,
    login VARCHAR(255),
    display_name VARCHAR(255),
    url TEXT
);

CREATE TABLE dim_repo (
    repo_sk INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    repo_id BIGINT UNIQUE NOT NULL,
    repo_name VARCHAR(255),
    repo_url TEXT
);

CREATE TABLE dim_event_type (
    event_type_sk INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_type_name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE dim_date (
    date_sk INTEGER PRIMARY KEY,
    date DATE UNIQUE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE fact_events (
    event_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id BIGINT UNIQUE NOT NULL,
    event_type_sk INTEGER NOT NULL REFERENCES dim_event_type(event_type_sk),
    actor_sk INTEGER NOT NULL REFERENCES dim_actor(actor_sk),
    repo_sk INTEGER NOT NULL REFERENCES dim_repo(repo_sk),
    date_sk INTEGER NOT NULL REFERENCES dim_date(date_sk),
    created_at TIMESTAMP NOT NULL
);
