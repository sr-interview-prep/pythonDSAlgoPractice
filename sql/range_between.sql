SELECT
    user_id,
    score,
    SUM(score) OVER (
        ORDER BY
            score RANGE BETWEEN 10 PRECEDING
            AND CURRENT ROW
    ) AS rolling_score
FROM
    scores;

-- it appears range between can only be used in case of numeric fields not dates