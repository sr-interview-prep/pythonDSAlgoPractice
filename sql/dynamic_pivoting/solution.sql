SELECT
  *
FROM
  salary_detail PIVOT(
    FOR
      gross_pay IN (
        SELECT DISTINCT
          gross_pay
        FROM
      )
  )
