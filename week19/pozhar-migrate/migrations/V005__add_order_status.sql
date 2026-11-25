-- UP
ALTER TABLE orders ADD COLUMN status TEXT NOT NULL DEFAULT 'new';
CREATE INDEX orders_status_idx ON orders (status);

-- DOWN
DROP INDEX orders_status_idx;
ALTER TABLE orders DROP COLUMN status;
