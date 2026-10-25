SELECT
    SUM(CASE WHEN scores_home_team  > scores_away_team  THEN 1 ELSE 0 END) AS home_wins_total,
    SUM(CASE WHEN scores_away_team  > scores_home_team  THEN 1 ELSE 0 END) AS away_wins_total
FROM {{ ref('stg_matches') }} m

