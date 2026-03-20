SELECT
  sale_date,
  revenue,
  revenue - LAG(revenue) OVER (ORDER BY sale_date) AS revenue_delta
FROM
  daily_sales
ORDER BY
  sale_date
LIMIT 3