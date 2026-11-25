-- UP
ALTER TABLE customers ADD COLUMN email TEXT;
CREATE UNIQUE INDEX customers_email_idx ON customers (email);

-- DOWN
DROP INDEX customers_email_idx;
ALTER TABLE customers DROP COLUMN email;
