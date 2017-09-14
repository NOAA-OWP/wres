-- Enumeration: Operating Member

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'operating_member') THEN
	CREATE TYPE operating_member AS ENUM ('left', 'right', 'baseline');
    END IF;
END $$;