WITH
  PARTICIPATIONS_ranked AS (
    SELECT
      *                 ,
      DENSE_RANK() OVER (
        PARTITION BY
          category
        ORDER BY
          score DESC       ,
          s.college_name ASC
      ) AS rnk
    FROM
      PARTICIPATIONS p
      INNER JOIN students s ON p.student_id = s.id
  )
SELECT
  category    ,
  student_id  ,
  name        ,
  college_name,
  score
FROM
  PARTICIPATIONS_ranked
WHERE
  rnk IN (1, 2, 3)
ORDER BY
  category    ,
  student_id  ,
  name        ,
  college_name,
  score
