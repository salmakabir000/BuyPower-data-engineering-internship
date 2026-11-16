-- UP
ALTER TABLE customers
ADD COLUMN email TEXT;

CREATE INDEX idx_customers_email
ON customers(email);

-- DOWN
DROP INDEX idx_customers_email;

ALTER TABLE customers
DROP COLUMN email;
