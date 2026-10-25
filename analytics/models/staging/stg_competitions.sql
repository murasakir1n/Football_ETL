{{
    config(
        materialized='view'
    )

}}

SELECT
    competition_id,
    competition_name
FROM {{ source('football_etl', 'competitions') }}
WHERE competition_id IS NOT NULL
