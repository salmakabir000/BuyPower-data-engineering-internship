-- UP
CREATE INDEX idx_customers_created_at
ON customers(created_at);

-- DOWN
DROP INDEX idx_customers_created_at;
