-- UP
ALTER TABLE customers ADD COLUMN phone TEXT;

-- DOWN
ALTER TABLE customers DROP COLUMN phone;
