-- A DDL for hosts_cumulated table
CREATE TABLE IF NOT EXISTS hosts_cumulated(
	host TEXT,
	user_id NUMERIC,
	host_activity_datelist DATE[],
	date DATE,
	PRIMARY KEY (host,user_id,date)
);

