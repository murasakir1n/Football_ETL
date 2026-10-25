{{
    config(
        materialized='view'
    )

}}

SELECT
    team_id,
    name,
    abbreviation,
    stadium,
    address,
    competition_id
FROM {{ source('football_data', 'teams') }}
WHERE team_id IS NOT NULL

