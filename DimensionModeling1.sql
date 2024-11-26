
-- create struts of temporal dimension/colums that change
-- CREATE TYPE season_stats AS (
-- 							season INTEGER,
-- 							gp REAL,
-- 							pts REAL,
-- 							reb REAL,
-- 							ast REAL
-- )

 -- CREATE TYPE scoring_class AS
 --     ENUM ('bad', 'average', 'good', 'star');
--drop table players
 -- CREATE TABLE players (
 --     player_name TEXT,
 --     height TEXT,
 --     college TEXT,
 --     country TEXT,
 --     draft_year TEXT,
 --     draft_round TEXT,
 --     draft_number TEXT,
 --     season_stats season_stats[],
 --     scoring_class scoring_class,
 --     years_since_last_active INTEGER,
 --     is_active BOOLEAN,
 --     current_season INTEGER,
 --     PRIMARY KEY (player_name, current_season)
 -- );

--select * from player_seasons
-- this is the seed query
WITH yesterday AS(

	SELECT * FROM players
	WHERE current_season = 2020
),
	today AS (
		SELECT * FROM player_seasons 
		WHERE season = 2021
	)
-- coalesce non-temporal column/dimensions(colums that doesn't change)
-- INSERT INTO players
-- SELECT
-- 	COALESCE(t.player_name,y.player_name)  player_name,
-- 	COALESCE(t.height,y.height) height,
-- 	COALESCE(t.college,y.college) college,
-- 	COALESCE(t.country,y.country) country,
-- 	COALESCE(t.draft_year,y.draft_year) draft_year,
-- 	COALESCE(t.draft_round,y.draft_round) draft_round,
-- 	COALESCE(t.draft_number,y.draft_number) draft_number,
-- 	CASE                                                 -- this 
-- 		WHEN y.seasons IS NULL
-- 		THEN ARRAY[ROW(
-- 			t.season,
-- 			t.gp,
-- 			t.pts,
-- 			t.reb,
-- 			t.ast
-- 		) :: season_stats]
-- 		WHEN t.season IS NOT NULL THEN y.seasons || ARRAY[ROW(
-- 			t.season,
-- 			t.gp,
-- 			t.pts,
-- 			t.reb,
-- 			t.ast
-- 		) :: season_stats]
-- 		ElSE y.seasons
-- 	END seasons,
-- 	CASE 
-- 		WHEN t.season IS NOT NULL THEN
-- 			CASE 
-- 				WHEN t.pts>20 THEN 'star'
-- 				WHEN t.pts>15 THEN 'good'
-- 				WHEN t.pts>10 THEN 'average'
-- 				ELSE 'bad'
-- 			END :: scoring_class
-- 		ELSE
-- 			y.scorer_class
-- 	END scorer_class,
-- 	CASE 
-- 		WHEN t.season IS NOT NULL THEN 0
-- 		ELSE COALESCE(y.years_since_last_active,0)+1
-- 	END years_since_last_active,
-- 	t.season IS NOT NULL as is_active,
-- 	COALESCE(t.season,y.current_season+1) current_season
-- FROM today t
-- FULL OUTER JOIN yesterday y
-- ON t.player_name=y.player_name;
-- SELECT * FROM players where  player_name = 'Aaron McKie';

INSERT INTO players
WITH years as(
	select *
	from generate_series(1996,2022)as season
	
),
	p as(
		select player_name,min(season) as first_season
		from player_seasons
		group by player_name
	),
	players_and_seasons as(
		select *
		from p  
			join years y on p.first_season<=y.season
	),
	windowed as(
		select ps.player_name,
				ps.season,
				array_remove(array_agg(case 
											when p1.season is not null then
												row(p1.season,p1.gp,p1.pts,p1.reb,p1.ast)::season_stats
										END)	
										OVER(partition by ps.player_name order by ps.season),null) as seasons
				from players_and_seasons ps
					left join player_seasons p1
						on ps.player_name=p1.player_name
						and ps.season=p1.season
				order by ps.player_name,ps.season
	),
	static as (
		select player_name,
				max(height) height,
				max(college) college,
				max(country) country,
				max(draft_year) draft_year,
				max(draft_round) draft_round,
				max(draft_number) draft_number
		from player_seasons
		group by player_name
				
	)

	select w.player_name,
			s.height,
			s.college,
			s.country,
			s.draft_year,
			s.draft_round,
			s.draft_number,
			w.seasons as season_stats,
			CASE
				WHEN (w.seasons[cardinality(seasons)]::season_stats).pts>20 THEN 'star'
				WHEN (w.seasons[cardinality(seasons)]::season_stats).pts>15 THEN 'good'
				WHEN (w.seasons[cardinality(seasons)]::season_stats).pts>10 THEN 'average'
				ELSE 'bad'
			END :: scoring_class as scorer_class,
				w.season - (w.seasons[cardinality(seasons)]::season_stats).season as years_since_last_active,
				(w.seasons[cardinality(seasons)]::season_stats).season =w.season as is_active,
				w.season
	FROM windowed w
	join static s on w.player_name=s.player_name

	select * from players
				
