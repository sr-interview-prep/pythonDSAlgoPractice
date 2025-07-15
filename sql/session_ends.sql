-- 1. User Session Gap
-- Given a table user_logins(user_id INT, login_time TIMESTAMP), identify sessions. A session ends if there's a gap of more than 30 minutes between logins for the same user. Assign session IDs.
WITH session_ended AS (
    SELECT
        user_id,
        login_time,
        CASE
            WHEN login_time - lag(login_time) over(
                PARTITION BY user_id
                ORDER BY
                    login_time
            ) > INTERVAL '30 minutes' THEN 1
            ELSE 0
        END AS session_ended
    FROM
        user_logins
)
SELECT
    user_id,
    login_time,
    sum(session_ended) over (
        PARTITION by user_id
        ORDER BY
            login_time
    ) AS session_id
FROM
    session_ended