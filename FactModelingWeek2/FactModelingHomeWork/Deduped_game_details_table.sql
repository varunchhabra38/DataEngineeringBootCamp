-- A query to deduplicate game_details from Day 1 so there's no duplicates

WITH
	deduped AS (
		SELECT
			*,
			-- Assign a unique row_num to each record
			ROW_NUMBER() OVER (
				PARTITION BY
					game_id,
					team_id,
					player_id
			) AS row_num
		FROM
			game_details
	),
	filtered_game_details AS(
		SELECT 
			game_id ,
			team_id ,
			team_abbreviation ,
			team_city ,
			player_id ,
			player_name ,
			nickname ,
			start_position ,
			comment ,
			min ,
			fgm ,
			fga ,
			fg_pct ,
			fg3m ,
			fg3a ,
			fg3_pct ,
			ftm ,
			fta ,
			ft_pct ,
			oreb ,
			dreb ,
			reb ,
			ast ,
			stl ,
			blk ,
			"TO" ,
			pf ,
			pts ,
			plus_minus
			
		FROM deduped 
		-- filtering only first row to eliminate duplicates
		WHERE row_num=1
	)

SELECT
	*
FROM
	filtered_game_details;


	