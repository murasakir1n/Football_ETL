{{
    config(
        materialized='view'
    )
}}

SELECT
    match_id,
    competition_id,
    home_team_id,
    home_team,
    away_team_id,
    away_team,
    scores_home_team,
    scores_away_team,
    referee_id,
    referee_name,
    CASE WHEN scores_home_team  > scores_away_team  THEN 1 ELSE 0 END AS home_wins,
    CASE WHEN scores_away_team  > scores_home_team  THEN 1 ELSE 0 END AS away_wins
FROM {{ source('football_data', 'matches')}}
WHERE scores_home_team IS NOT NULL
    AND scores_away_team IS NOT NULL


