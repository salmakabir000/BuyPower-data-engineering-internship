-- UP
ALTER TABLE customers
ADD COLUMN city TEXT;

-- DOWN
ALTER TABLE customers
DROP COLUMN city;
