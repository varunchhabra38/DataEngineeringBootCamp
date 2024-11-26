

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



-- Query to load data in into cumulative table (actors)

DO $$
	DECLARE
		year_start INTEGER:= 1970;
		year_end INTEGER:= 2021;
	BEGIN
		FOR st_year IN year_start..year_end LOOP		
			INSERT INTO	ACTORS
				-- selecting last year data, which is the entire historical data already present in actors table
			WITH
				LAST_YEAR AS (
					SELECT
						*
					FROM
						ACTORS
					WHERE
						YEAR = st_year
				),
				-- selecting current year data from actor_films table and which will be loaded to actors table
				THIS_YEAR AS (
					SELECT
						ACTORID,
						ACTOR,
						YEAR,
						ARRAY_AGG(ROW (FILM, VOTES, RATING, FILMID)::FILMS) FILMS, -- creating an array of all the films for an actor in a year
						ROUND(AVG(RATING)::NUMERIC, 2) AVG_RATING
					FROM
						ACTOR_FILMS
					WHERE
						YEAR = st_year+1
					GROUP BY
						ACTORID,
						ACTOR,
						YEAR
				)
			SELECT
				COALESCE(T.ACTORID, Y.ACTORID) ACTORID, --coalese for non-temporal columns, to remove nulls
				COALESCE(T.ACTOR, Y.ACTOR) ACTOR,
				CASE
					WHEN Y.FILMS IS NULL
					AND T.FILMS IS NOT NULL -- if not data exists in the actors table for and an actor, just add data from actor_films for the current year 
					THEN T.FILMS
					WHEN T.YEAR IS NOT NULL THEN Y.FILMS || T.FILMS -- if films exisits already then append it to exisitng films array
					ELSE Y.FILMS
				END FILMS,
				CASE
					WHEN T.FILMS IS NOT NULL THEN CASE
						WHEN T.AVG_RATING > 8 THEN 'star'
						WHEN T.AVG_RATING > 7 THEN 'good'
						WHEN T.AVG_RATING > 6 THEN 'average'
						ELSE 'bad'
					END::QUALITY_CLASS
					ELSE Y.QUALITY_CLASS
				END QUALITY_CLASS,
				T.YEAR IS NOT NULL IS_ACTIVE,
				COALESCE(T.YEAR, Y.YEAR + 1) AS YEAR
			FROM
				THIS_YEAR T
				FULL OUTER JOIN LAST_YEAR Y ON T.ACTORID = Y.ACTORID
			ON CONFLICT(actorid,year) DO UPDATE
			SET actorid=EXCLUDED.actorid,
				year=EXCLUDED.year;
		END LOOP;
END $$;


--- SCD 

INSERT INTO	ACTORS_HISTORY_SCD
	WITH STREAK_STARTED AS (
			SELECT
				ACTORID,
				ACTOR,
				QUALITY_CLASS,
				IS_ACTIVE,
				YEAR,
				LAG(QUALITY_CLASS, 1) OVER ( PARTITION BY ACTORID ORDER BY YEAR) <> QUALITY_CLASS
				OR LAG(QUALITY_CLASS, 1) OVER (PARTITION BY ACTORID ORDER BY YEAR) IS NULL
				OR LAG(IS_ACTIVE, 1) OVER (	PARTITION BY ACTORID ORDER BY YEAR) <> IS_ACTIVE
				OR LAG(IS_ACTIVE, 1) OVER (	PARTITION BY ACTORID ORDER BY YEAR) IS NULL AS DID_CHANGE
			FROM
				ACTORS
			WHERE YEAR<2021
		),
		STREAK_IDENTIFIED AS (
			SELECT
				ACTORID,
				ACTOR,
				QUALITY_CLASS,
				IS_ACTIVE,
				YEAR,
				SUM(CASE
						WHEN DID_CHANGE THEN 1
						ELSE 0
					END
				) OVER (PARTITION BY ACTORID	ORDER BY	YEAR) AS STREAK_IDENTIFIER
			FROM
				STREAK_STARTED
		),
		AGGREGATED AS (
			SELECT
				ACTORID,
				ACTOR,
				QUALITY_CLASS,
				IS_ACTIVE,
				STREAK_IDENTIFIER,
				MIN(YEAR) AS START_DATE,
				MAX(YEAR) AS END_DATE
			FROM
				STREAK_IDENTIFIED
			GROUP BY
				ACTORID,ACTOR,QUALITY_CLASS,IS_ACTIVE,STREAK_IDENTIFIER
		)
		
	SELECT
		ACTORID,
		ACTOR,
		QUALITY_CLASS,
		IS_ACTIVE,
		START_DATE,
		END_DATE,
		2020 as CURRENT_YEAR
	FROM
		AGGREGATED
	ORDER BY
		ACTOR,
		START_DATE;

select * from ACTORS_HISTORY_SCD


---Incremental SCD

WITH last_year_scd AS (
    SELECT * FROM ACTORS_HISTORY_SCD
    WHERE start_date = 2020
    AND end_date = 2020
),
     historical_scd AS (
        SELECT
	           actorid,
               actor,
               quality_class,
               is_active,
               start_date,
			   end_date
        FROM ACTORS_HISTORY_SCD
        WHERE end_date <= 2020
        AND start_date < 2020
     ),
     this_year_data AS (
         SELECT * FROM actors
         WHERE year = 2021
     ),
     unchanged_records AS (
         SELECT
	           ts.actorid,
               ts.actor,
               ts.quality_class,
               ts.is_active,
               ls.start_date ,
			   ts.year as end_date
        FROM this_year_data ts
        JOIN last_year_scd ls
        ON ls.actorid = ts.actorid
         WHERE ts.quality_class = ls.quality_class
         AND ts.is_active = ls.is_active
     ),
     changed_records AS (
        SELECT
                ts.actorid,
				ts.actor,
                UNNEST(ARRAY[
                    ROW(
                        ls.quality_class,
                        ls.is_active,
                        ls.start_date,
                        ls.end_date
                        )::scd_actor_type,
                    ROW(
                        ts.quality_class,
                        ts.is_active,
                        ts.year,
                        ts.year
                        )::scd_actor_type
                ]) as records
        FROM this_year_data ts
        LEFT JOIN last_year_scd ls
        ON ls.actorid = ts.actorid
         WHERE (ts.quality_class <> ls.quality_class
          OR ts.is_active <> ls.is_active)
     ),
     unnested_changed_records AS (

         SELECT actorid,
		 		actor,
                (records::scd_actor_type).quality_class,
                (records::scd_actor_type).is_active,
                (records::scd_actor_type).start_date,
                (records::scd_actor_type).end_date
                FROM changed_records
         ),
     new_records AS (

         SELECT
	           ts.actorid,
               ts.actor,
               ts.quality_class,
               ts.is_active,
               ts.year as start_date,
			   ts.year as end_date
         FROM this_year_data ts
         LEFT JOIN last_year_scd ls
             ON ts.actorid = ls.actorid
         WHERE ls.actorid IS NULL

     )


SELECT *, 2022 AS current_season FROM (
                  SELECT *
                  FROM historical_scd

                  UNION ALL

                  SELECT *
                  FROM unchanged_records

                  UNION ALL

                  SELECT *
                  FROM unnested_changed_records

                  UNION ALL

                  SELECT *
                  FROM new_records
              ) a









