WITH t AS (

	SELECT competition_id, home_team AS team,
			scores_home_team AS sh, scores_away_team AS sa
	FROM {{ source('football_data', 'matches') }}
	UNION ALL
	SELECT competition_id, away_team,
			scores_away_team, scores_home_team
	FROM {{ source('football_data', 'matches') }} m
)

SELECT competition_id, team,
       COUNT(*)                                   AS played,
       SUM(CASE WHEN sh > sa THEN 1 ELSE 0 END)   AS wins,
       SUM(CASE WHEN sh = sa THEN 1 ELSE 0 END)   AS draws,
       SUM(CASE WHEN sh < sa THEN 1 ELSE 0 END)   AS losses,
       SUM(sh) - SUM(sa)                          AS goal_diff,
       SUM(CASE WHEN sh > sa THEN 3 WHEN sh = sa THEN 1 ELSE 0 END) AS points
FROM t
GROUP BY competition_id, team
ORDER BY competition_id, points DESC, goal_diff DESC


