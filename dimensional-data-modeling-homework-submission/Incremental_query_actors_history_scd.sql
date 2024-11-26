---Incremental SCD

INSERT INTO ACTORS_HISTORY_SCD
WITH last_year_scd AS (
    SELECT * FROM ACTORS_HISTORY_SCD
    WHERE CURRENT_YEAR = 2020
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
        WHERE CURRENT_YEAR <= 2020
        AND END_DATE < 2020
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









