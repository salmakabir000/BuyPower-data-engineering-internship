-- UP
CREATE TABLE customers (
    customer_id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- DOWN
DROP TABLE customers;
