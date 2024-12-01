-- Method 1

INSERT INTO players
SELECT
	COALESCE(t.player_name,y.player_name)  player_name,
	COALESCE(t.height,y.height) height,
	COALESCE(t.college,y.college) college,
	COALESCE(t.country,y.country) country,
	COALESCE(t.draft_year,y.draft_year) draft_year,
	COALESCE(t.draft_round,y.draft_round) draft_round,
	COALESCE(t.draft_number,y.draft_number) draft_number,
	CASE                                                 -- this 
		WHEN y.seasons IS NULL
		THEN ARRAY[ROW(
			t.season,
			t.gp,
			t.pts,
			t.reb,
			t.ast
		) :: season_stats]
		WHEN t.season IS NOT NULL THEN y.seasons || ARRAY[ROW(
			t.season,
			t.gp,
			t.pts,
			t.reb,
			t.ast
		) :: season_stats]
		ElSE y.seasons
	END seasons,
	CASE 
		WHEN t.season IS NOT NULL THEN
			CASE 
				WHEN t.pts>20 THEN 'star'
				WHEN t.pts>15 THEN 'good'
				WHEN t.pts>10 THEN 'average'
				ELSE 'bad'
			END :: scoring_class
		ELSE
			y.scorer_class
	END scorer_class,
	CASE 
		WHEN t.season IS NOT NULL THEN 0
		ELSE COALESCE(y.years_since_last_active,0)+1
	END years_since_last_active,
	t.season IS NOT NULL as is_active,
	COALESCE(t.season,y.current_season+1) current_season
FROM today t
FULL OUTER JOIN yesterday y
ON t.player_name=y.player_name;

-- Method 2


INSERT INTO players
WITH years AS (
    SELECT *
    FROM GENERATE_SERIES(1996, 2022) AS season
), p AS (
    SELECT
        player_name,
        MIN(season) AS first_season
    FROM player_seasons
    GROUP BY player_name
), players_and_seasons AS (
    SELECT *
    FROM p
    JOIN years y
        ON p.first_season <= y.season
), windowed AS (
    SELECT
        pas.player_name,
        pas.season,
        ARRAY_REMOVE(
            ARRAY_AGG(
                CASE
                    WHEN ps.season IS NOT NULL
                        THEN ROW(
                            ps.season,
                            ps.gp,
                            ps.pts,
                            ps.reb,
                            ps.ast
                        )::season_stats
                END)
            OVER (PARTITION BY pas.player_name ORDER BY COALESCE(pas.season, ps.season)),
            NULL
        ) AS seasons
    FROM players_and_seasons pas
    LEFT JOIN player_seasons ps
        ON pas.player_name = ps.player_name
        AND pas.season = ps.season
    ORDER BY pas.player_name, pas.season
), static AS (
    SELECT
        player_name,
        MAX(height) AS height,
        MAX(college) AS college,
        MAX(country) AS country,
        MAX(draft_year) AS draft_year,
        MAX(draft_round) AS draft_round,
        MAX(draft_number) AS draft_number
    FROM player_seasons
    GROUP BY player_name
)
SELECT
    w.player_name,
    s.height,
    s.college,
    s.country,
    s.draft_year,
    s.draft_round,
    s.draft_number,
    seasons AS season_stats,
    CASE
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 20 THEN 'star'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 15 THEN 'good'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 10 THEN 'average'
        ELSE 'bad'
    END::scoring_class AS scoring_class,
    w.season - (seasons[CARDINALITY(seasons)]::season_stats).season as years_since_last_active,
    w.season,
    (seasons[CARDINALITY(seasons)]::season_stats).season = season AS is_active
FROM windowed w
JOIN static s
    ON w.player_name = s.player_name;