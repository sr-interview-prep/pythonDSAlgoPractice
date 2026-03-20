WITH m AS (
  SELECT DISTINCT
    user_id,
    DATE_TRUNC('month', event_date) AS month
  FROM user_actions
  WHERE event_type IN ('sign-in', 'like', 'comment')
),
active AS (
  SELECT c.user_id, c.month
  FROM m c
  JOIN m p
    ON p.user_id = c.user_id
   AND p.month = c.month - INTERVAL '1 month'
)
SELECT
  EXTRACT(month FROM month) AS mth,
  COUNT(DISTINCT user_id) AS monthly_active_users
FROM active
WHERE month = DATE '2022-07-01'
GROUP BY 1;