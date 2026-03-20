WITH
  ordered_products_cum_sum AS (
    SELECT
      *               ,
      SUM(price) OVER (
        ORDER BY
          price
      ) AS cumulative_sum
    FROM
      products
    ORDER BY
      price
  )
SELECT
  c.name  ,
  c.budget,
  p.item  ,
  p.price
FROM
  customers c
  LEFT JOIN ordered_products_cum_sum p ON c.budget >= p.cumulative_sum
