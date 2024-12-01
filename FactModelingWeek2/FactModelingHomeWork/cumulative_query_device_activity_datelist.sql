
DO $$
	DECLARE

	BEGIN
		FOR st IN 0..30 LOOP
		
		-- Insert the cumulative data into the user_devices_cumulated table

			INSERT INTO user_devices_cumulated
			WITH yesterday AS (
			    -- Fetch the previously cumulated data for the last known date
			    SELECT * 
			    FROM user_devices_cumulated
			    WHERE date = DATE('2022-12-31') + st -- Replace with the previous day's date dynamically as needed
			),
			today AS (
			    -- Fetch today's activity, grouped by user_id and browser_type
			    SELECT 
			        e.user_id,  -- User identifier
			        d.browser_type,  -- Browser type from devices table
			        DATE_TRUNC('day', DATE(e.event_time)) AS today_date,  -- Extract the date part from event_time
			        COUNT(1) AS num_events  -- Count number of events for each user and browser type
			    FROM 
			        events e
			    JOIN 
			        devices d 
			    ON 
			        e.device_id = d.device_id  -- Join events with devices to get the browser_type
			    WHERE 
			        DATE_TRUNC('day', DATE(e.event_time)) = DATE('2023-01-01')+st  -- Replace with today's date dynamically
			        AND e.user_id IS NOT NULL  -- Ensure the user_id is not null
			    GROUP BY 
			        e.user_id, d.browser_type, DATE_TRUNC('day', DATE(e.event_time))  -- Group by user, browser, and date
			)
			
			SELECT
			    -- Combine user_id from yesterday and today (if available)
			    COALESCE(t.user_id::TEXT, y.user_id) AS user_id,
			    
			    -- Combine browser_type from yesterday and today (if available)
			    COALESCE(t.browser_type, y.browser_type) AS browser_type,

				-- Use today's date if available, otherwise increment yesterday's date by one day
			    COALESCE(t.today_date, y.date + INTERVAL '1 day') AS date,
			    -- Combine the previous device_activity_datelist with today's date, if there's new activity
			    COALESCE(y.device_activity_datelist, ARRAY[]::DATE[]) 
			    || CASE 
			           WHEN t.user_id IS NOT NULL THEN ARRAY[t.today_date]  -- Add today's date if there are new events for this user and browser
			           ELSE ARRAY[]::DATE[]  -- Otherwise, add an empty array
			       END AS device_activity_datelist		   

			FROM 
			    yesterday y  -- Data from the previous day's cumulated results
			FULL OUTER JOIN 
			    today t  -- Data from today's events
			ON 
			    t.user_id :: TEXT = y.user_id  -- Join on user_id
			    AND t.browser_type = y.browser_type;  -- Join on browser_type
		END LOOP;
END $$;







