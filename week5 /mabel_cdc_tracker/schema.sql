-- schema.sql
-- Run this once against a fresh Postgres instance to create the CDC source table.

CREATE TABLE IF NOT EXISTS customers (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    email       TEXT NOT NULL,
    city        TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at  TIMESTAMPTZ
);

-- Index on the watermark column(s) — this is what makes the polling query cheap.
-- Without it, every 30s poll is a full table scan.
CREATE INDEX IF NOT EXISTS idx_customers_updated_at_id ON customers (updated_at, id);
