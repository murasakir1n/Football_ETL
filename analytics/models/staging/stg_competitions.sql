{{
    config(
        materialized='view'
    )

}}

SELECT
    competition_id,
    competition_name
FROM {{ source('football_etl', 'competition') }}
WHERE competition_id IS NOT NULL
