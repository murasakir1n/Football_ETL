{{
    config(
        materialized='view'
    )

}}

SELECT
    competition_id,
    competition_name
FROM {{ source('football_data', 'competition') }}
WHERE competition_id IS NOT NULL
