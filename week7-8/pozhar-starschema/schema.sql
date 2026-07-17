-- dim_event_type: lookup table for event types
CREATE TABLE dim_event_type (
    event_type_sk SERIAL PRIMARY KEY,
    event_type_name TEXT NOT NULL UNIQUE
);

-- dim_actor: one row per unique GitHub user
CREATE TABLE dim_actor (
    actor_sk SERIAL PRIMARY KEY,
    actor_id BIGINT NOT NULL UNIQUE,
    login TEXT NOT NULL,
    display_name TEXT,
    url TEXT
);

-- dim_repo: one row per unique repository
CREATE TABLE dim_repo (
    repo_sk SERIAL PRIMARY KEY,
    repo_id BIGINT NOT NULL UNIQUE,
    repo_name TEXT NOT NULL,
    repo_url TEXT
);

-- dim_date: one row per calendar date (10 years)
CREATE TABLE dim_date (
    date_sk INTEGER PRIMARY KEY,  -- YYYYMMDD format
    date DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name TEXT NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name TEXT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- fact_events: one row per GitHub event
CREATE TABLE fact_events (
    event_sk SERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    event_type_sk INTEGER REFERENCES dim_event_type(event_type_sk),
    actor_sk INTEGER REFERENCES dim_actor(actor_sk),
    repo_sk INTEGER REFERENCES dim_repo(repo_sk),
    date_sk INTEGER REFERENCES dim_date(date_sk),
    created_at TIMESTAMPTZ NOT NULL
);
