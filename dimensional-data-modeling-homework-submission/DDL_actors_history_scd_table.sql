-- create actors_history_scd table 

DO $$
	BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'scd_actor_type') THEN
		CREATE TYPE scd_actor_type AS (
		                    quality_class quality_class,
		                    is_active boolean,
		                    start_date INTEGER,
		                    end_date INTEGER
							);
END IF;
END $$;

CREATE TABLE IF NOT EXISTS ACTORS_HISTORY_SCD (
	ACTORID TEXT,
	ACTOR TEXT,
	QUALITY_CLASS quality_class,
	IS_ACTIVE BOOLEAN,
	START_DATE INTEGER,
	END_DATE INTEGER,
	CURRENT_YEAR INTEGER
);