WITH exchange_rate_updated AS (
  SELECT
    *,
    LEAD(effective_start_date) OVER (
      PARTITION BY source_currency
      ORDER BY
        effective_start_date
    ) AS effective_end_date
  FROM
    exchange_rate
  WHERE
    target_currency = 'USD'
),
non_usd_sales AS (
  SELECT
    sales_date,
    ROUND(
      SUM(sales_amount * exchange_rate),
      2
    ) AS sales_amount
  FROM
    sales_amount s
    INNER JOIN exchange_rate_updated e ON s.sales_date >= e.effective_start_date
    AND s.sales_date < COALESCE(
      e.effective_end_date,
      '2099-01-01'
    )
    AND s.currency = e.source_currency
  WHERE
    s.currency <> 'USD'
  GROUP BY
    sales_date
),
usd_sales AS (
  SELECT
    sales_date,
    ROUND(SUM(sales_amount), 2) AS sales_amount
  FROM
    sales_amount s
  WHERE
    s.currency = 'USD'
  GROUP BY
    sales_date
)
SELECT
  sales_date,
  SUM(sales_amount) AS sales_amount
FROM
  (
    SELECT
      *
    FROM
      non_usd_sales
    UNION
    ALL
    SELECT
      *
    FROM
      usd_sales
  ) t
GROUP BY
  sales_date
ORDER BY
  sales_date
