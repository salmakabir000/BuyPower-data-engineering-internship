-- Target dimension table for the SCD Type 2 (and Type 1) exercises.
-- customer_sk is the surrogate key (one row per version, per customer_id).
-- customer_id is the business/natural key coming from the source system.

DROP TABLE IF EXISTS dim_customer;

CREATE TABLE dim_customer (
    customer_sk SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    name        TEXT NOT NULL,
    email       TEXT NOT NULL,
    city        TEXT,
    valid_from  TIMESTAMPTZ NOT NULL,
    valid_to    TIMESTAMPTZ,
    is_current  BOOLEAN NOT NULL DEFAULT TRUE
);

-- Helpful for the "one current row per customer" invariant and for lookups.
CREATE INDEX idx_dim_customer_business_key ON dim_customer (customer_id);
CREATE UNIQUE INDEX uq_dim_customer_current
    ON dim_customer (customer_id)
    WHERE is_current = TRUE;

-- Separate table used for the Type 1 exercise so the two approaches never
-- collide with each other (Type 1 has no history, so we don't need
-- valid_from/valid_to/is_current at all, but we keep the same shape plus a
-- last_updated column so you can see what changed and when).

DROP TABLE IF EXISTS dim_customer_scd1;

CREATE TABLE dim_customer_scd1 (
    customer_id INT PRIMARY KEY,
    name        TEXT NOT NULL,
    email       TEXT NOT NULL,
    city        TEXT,
    last_updated TIMESTAMPTZ NOT NULL
);

-- Stretch goal: Type 4 uses a small "current" table plus a full-row
-- history table. The current table has the same shape as dim_customer_scd1
-- (no valid_from/to needed, since "current" always means "current").

DROP TABLE IF EXISTS dim_customer_current;
DROP TABLE IF EXISTS dim_customer_history;

CREATE TABLE dim_customer_current (
    customer_id INT PRIMARY KEY,
    name        TEXT NOT NULL,
    email       TEXT NOT NULL,
    city        TEXT,
    last_updated TIMESTAMPTZ NOT NULL
);

CREATE TABLE dim_customer_history (
    history_sk  SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    name        TEXT NOT NULL,
    email       TEXT NOT NULL,
    city        TEXT,
    recorded_at TIMESTAMPTZ NOT NULL
);
