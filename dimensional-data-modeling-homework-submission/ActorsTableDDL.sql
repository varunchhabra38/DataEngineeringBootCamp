-- create struts for temporal columns
DO $$
	BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'films') THEN
		CREATE TYPE films AS (
			film TEXT,
			votes INTEGER,
			rating REAL,
			filmid TEXT
		);
	END IF;
	END $$;

--create enum for quality class
DO $$
	BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'quality_class') THEN
		CREATE TYPE quality_class AS
			ENUM ('bad', 'average', 'good', 'star');
	END IF;
	END $$;

--create actor table if not exists, this will me a cumulative table
CREATE TABLE IF NOT EXISTS ACTORS (
	ACTORID TEXT,
	ACTOR TEXT,
	FILMS FILMS[],
	QUALITY_CLASS QUALITY_CLASS,
	IS_ACTIVE BOOLEAN,
	YEAR INTEGER,
	PRIMARY KEY (ACTORID, YEAR)
);