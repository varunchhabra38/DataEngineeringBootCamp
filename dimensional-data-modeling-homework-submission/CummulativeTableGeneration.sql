
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
				FULL OUTER JOIN LAST_YEAR Y ON T.ACTORID = Y.ACTORID;
		END LOOP;
END $$;