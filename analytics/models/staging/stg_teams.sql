{{
    config(
        materialized='view'
    )

}}

SELECT
    team_id,
    name,
    abreviation,
    stadium,
    address,
    competition_id
FROM {{ source('football_etl', 'teams') }}
WHERE team_id IS NOT NULL

