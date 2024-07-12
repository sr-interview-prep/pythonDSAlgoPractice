WITH
  updated_transaction_table AS (
    SELECT
      CONCAT(
        '20'                          ,
        SUBSTRING(transaction_id, 1, 2)
      ) AS transaction_year,
      transaction_id       ,
      user_id
    FROM
      transactions t
    WHERE
      CONCAT(
        '20'                          ,
        SUBSTRING(transaction_id, 1, 2)
      ) IN ('2019', '2020', '2021')
  )
SELECT
  user_name,
  SUM(
    CASE
      WHEN transaction_year = '2019' THEN 1
      ELSE 0
    END
  ) AS nineteen,
  SUM(
    CASE
      WHEN transaction_year = '2020' THEN 1
      ELSE 0
    END
  ) AS twenty,
  SUM(
    CASE
      WHEN transaction_year = '2021' THEN 1
      ELSE 0
    END
  ) AS twentyone
FROM
  updated_transaction_table t
  INNER JOIN users u ON t.user_id = u.user_id
GROUP BY
  user_name
ORDER BY
  user_name
