-- A DDL for an user_devices_cumulated table that has:
-- a device_activity_datelist which tracks a users active days by browser_type
-- data type here should look similar to MAP<STRING, ARRAY[DATE]>
-- or you could have browser_type as a column with multiple rows for each user (either way works, just be consistent!)

CREATE TABLE IF NOT EXISTS user_devices_cumulated (
	user_id  TEXT, -- unique identifier for user
	browser_type TEXT, -- type of browser used by the user,
	date DATE, -- date of event
	device_activity_datelist DATE[], --  List of active days for the browser
	PRIMARY KEY(user_id,browser_type,date)
	
)
