-- UP
CREATE INDEX idx_customers_name ON customers (name);

-- DOWN
DROP INDEX idx_customers_name;
