-- UP
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

-- DOWN
DROP TABLE customers;
