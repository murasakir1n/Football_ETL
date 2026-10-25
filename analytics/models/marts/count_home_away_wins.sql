SELECT
    SUM(home_wins) AS home_wins_total,
    SUM(away_wins) AS away_wins_total
FROM {{ ref('stg_matches') }} m

