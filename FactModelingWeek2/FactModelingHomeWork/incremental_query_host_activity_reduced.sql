-- Incrementally loads daily activity into the host_activity_reduced table
INSERT INTO host_activity_reduced
WITH
    -- Step 1: Aggregate daily activity from the events table
    daily_activity AS (
        SELECT
            host,                                        -- The host being accessed
            COUNT(DISTINCT user_id) AS daily_unique_visitors, -- Count of unique users for the host on the given date
            COUNT(1) AS daily_hits,                     -- Total number of hits for the host on the given date
            DATE(event_time) AS date                    -- Extract the date from event_time
        FROM
            events
        WHERE
            DATE(event_time) = ('2023-01-10')           -- Filter events for the target day (incremental)
            AND user_id IS NOT NULL                     -- Ensure user_id is not null
            AND host IS NOT NULL                        -- Ensure host is not null
        GROUP BY
            host,                                       -- Group data by host
            DATE(event_time)                            -- And by date
    ),
    -- Step 2: Fetch the existing monthly data from the host_activity_reduced table
    yesterday AS (
        SELECT
            *                                           -- Select all columns
        FROM
            host_activity_reduced
    )
-- Step 3: Merge new daily activity with existing data
SELECT
    COALESCE(y.host, d.host) AS host,                   -- Use host from either daily_activity or yesterday
    DATE(COALESCE(y.month, DATE_TRUNC('month', d.date))) AS month, -- Determine the month of the data
    -- Build or extend the hit_array by appending the daily hits
    COALESCE(y.hit_array, 
        ARRAY_FILL(0, ARRAY[COALESCE(d.date - DATE(DATE_TRUNC('month', d.date)), 0)])) -- Fill missing days with 0
        || ARRAY[COALESCE(d.daily_hits, 0)] AS hit_array, -- Append the current day's hits
    -- Build or extend the unique_visitors array by appending daily unique visitors
    COALESCE(y.unique_visitors, 
        ARRAY_FILL(0, ARRAY[COALESCE(d.date - DATE(DATE_TRUNC('month', d.date)), 0)])) -- Fill missing days with 0
        || ARRAY[COALESCE(d.daily_unique_visitors, 0)] AS unique_visitors -- Append the current day's unique visitors
FROM
    yesterday y
FULL OUTER JOIN                                        -- Join the existing data with the new daily activity
    daily_activity d
ON 
    d.host = y.host                                    -- Match on the host
    AND y.month = DATE_TRUNC('month', d.date)          -- Match on the same month
-- Handle conflicts during insert
ON CONFLICT (host, month)
DO 
    -- Update the hit_array and unique_visitors arrays to include the new data
    UPDATE SET 
        hit_array = EXCLUDED.hit_array,                -- Replace hit_array with the updated array
        unique_visitors = EXCLUDED.unique_visitors;    -- Replace unique_visitors with the updated array
