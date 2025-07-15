-- Rolling 7 - DAY Active Users Given a TABLE 
-- user_activity(user_id, activity_date),
-- WRITE a query TO get each date
-- AND the count of DISTINCT users active IN the past 7 days (inclusive) of that date.
-- Expected COLUMNS: activity_date, active_users_last_7_days
WITH activity_dates AS (
    SELECT
        DISTINCT activity_date
    FROM
        user_activity
)
SELECT
    activity_date,
    count(DISTINCT user_id) AS active_users_last_7_days
FROM
    activity_dates ad
    LEFT JOIN user_activity ua ON ua.activity_date BETWEEN ad.activity_date - INTERVAL '6 days'
    AND ad.activity_date
GROUP BY
    activity_date
ORDER BY
    activity_date DESC