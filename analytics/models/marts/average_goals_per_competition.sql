SELECT c.competition_id,
       c.competition_name,
        ROUND
        (AVG(scores_home_team + scores_away_team),
         1) AS avg_goals
FROM {{ source('football_data', 'competitions') }} c
JOIN {{ source('football_data', 'matches') }} m
    ON c.competition_id = m.competition_id
GROUP BY c.competition_id,
         c.competition_name
ORDER BY avg_goals desc