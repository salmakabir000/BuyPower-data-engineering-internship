-- UP
ALTER TABLE customers ADD COLUMN nickname TEXT;

-- DOWN
ALTER TABLE customers DROP COLUMN nickname;
