{{
    config(
        materialized='view'
    )

}}

SELECT
    player_id,
    name,
    team_id,
    position,
    date_of_birth,
    nationality,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, date_of_birth))::INTEGER AS age
FROM {{ source('football_etl', 'players') }}
WHERE date_of_birth IS NOT NULL


