SELECT nationality, ROUND(
        AVG(EXTRACT(YEAR FROM AGE(date_of_birth)))::numeric, 1) AS age
FROM {{ ref('stg_players') }} p
WHERE date_of_birth IS NOT NULL
  AND EXTRACT(YEAR FROM AGE(date_of_birth)) >= 18
GROUP BY nationality
ORDER BY age DESC
limit 10
