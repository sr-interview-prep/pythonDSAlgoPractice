WITH
  visitors_lead AS (
    SELECT
      id                       ,
      event_date               ,
      LAG(no_of_visitors) OVER (
        ORDER BY
          id
      ) AS lag_1                  ,
      LAG(no_of_visitors, 2) OVER (
        ORDER BY
          id
      ) AS lag_2                ,
      no_of_visitors AS curr    ,
      LEAD(no_of_visitors) OVER (
        ORDER BY
          id
      ) AS lead_1                  ,
      LEAD(no_of_visitors, 2) OVER (
        ORDER BY
          id
      ) AS lead_2
    FROM
      visitors
  )
SELECT
  id                         ,
  event_date                 ,
  curr       AS no_of_visitors
FROM
  visitors_lead
WHERE
  (
    lag_2 >= 100
    AND lag_1 >= 100
    AND curr >= 100
  )
  OR (
    lag_1 >= 100
    AND curr >= 100
    AND lead_1 >= 100
  )
  OR (
    curr >= 100
    AND lead_1 >= 100
    AND lead_2 >= 100
  )
