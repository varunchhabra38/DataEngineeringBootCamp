-- A datelist_int generation query. Convert the device_activity_datelist column into a datelist_int column

	-- Step 1: Prepare data for bitwise conversion
	WITH starter AS (
	-- Check if the user was active on the given valid_date
    SELECT uc.device_activity_datelist @> ARRAY [DATE(d.valid_date)]   AS is_active,
			device_activity_datelist,
			browser_type,
			-- extract day from the date
           EXTRACT(
               DAY FROM  d.valid_date) AS day_active,
           uc.user_id
    FROM user_devices_cumulated uc
             CROSS JOIN
		-- Generate a series of dates between 2023-01-01 and 2023-01-31
         (SELECT generate_series('2023-01-01', '2023-01-31', INTERVAL '1 day') AS valid_date) as d
	-- Filter to include only rows with the date equal to 2023-03-31
    WHERE date = DATE('2023-01-31')
),
	-- Step 2: Convert the active dates into a 32-bit integer
     bits AS (
         SELECT user_id,
		 		browser_type,
				 -- Sum the bitwise representation of active dates
				SUM(CASE
                        WHEN is_active THEN POW(2, 32 - day_active) -- consider for bit conversion if user is active on that day
                        ELSE 0 	-- No contribution if the user was not active on that date
					END)::bigint AS datelist_int, 
                SUM(CASE
                        WHEN is_active THEN POW(2, 32 - day_active )
                        ELSE 0 
					END)::bigint::bit(32) AS datelist_bit,-- converting bitint to 32 bit representation
						
                DATE('2023-01-31') as date
         FROM starter
         GROUP BY user_id,browser_type
     )
 select * from bits  order by user_id,date