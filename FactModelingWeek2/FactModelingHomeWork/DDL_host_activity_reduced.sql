-- DDL host_activity_reduced
--drop table host_activity_reduced
-- CREATE TABLE IF NOT EXISTS host_activity_reduced(
-- 	host TEXT,
-- 	month DATE,
-- 	hit_array BIGINT[],
-- 	unique_visitors NUMERIC[],
-- 	PRIMARY KEY(host,month)
-- )
--DELETE FROM host_activity_reduced
INSERT INTO host_activity_reduced
WITH
	daily_activity AS (
		SELECT
			host,
			COUNT(DISTINCT user_id) as daily_unique_visitors,
			COUNT(1) AS daily_hits,
			DATE (event_time) AS date
		FROM
			events
		WHERE
			DATE (event_time) = ('2023-01-01')
			AND user_id IS NOT NULL
			AND host IS NOT NULL
		GROUP BY
			host,
			DATE (event_time)
	),
	yesterday AS (
		SELECT
			*
		FROM
			host_activity_reduced
	)
SELECT
	COALESCE(y.host, d.host) AS host,
	DATE(COALESCE(y.month, DATE_TRUNC('month', d.date))) AS MONTH,
	COALESCE(y.hit_array,ARRAY_FILL(0, ARRAY[COALESCE(d.date - DATE(DATE_TRUNC('month', d.date)),0)]))
        || ARRAY[COALESCE(d.daily_hits,0)]AS hits_array,
	COALESCE(y.unique_visitors,ARRAY_FILL(0, ARRAY[COALESCE(d.date - DATE(DATE_TRUNC('month', d.date)),0)]))
        || ARRAY[COALESCE(d.daily_unique_visitors,0)] AS unique_visitors
FROM
	yesterday y
FULL OUTER JOIN 
	daily_activity d
ON 
	d.host=y.host
AND y.month=DATE_TRUNC('month', d.date)
	
ON CONFLICT (host,month)
DO 
	UPDATE SET 
	hit_array=EXCLUDED.hit_array,
	unique_visitors=EXCLUDED.unique_visitors;
		
	
		
-- SELECT * FROM host_activity_reduced