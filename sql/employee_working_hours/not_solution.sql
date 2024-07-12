WITH
  employee_attendance_with_lead_result AS (
    SELECT
      *                  ,
      LEAD(at_date) OVER (
        PARTITION BY
          employee_id
        ORDER BY
          at_time
      ) AS lead_at_date  ,
      LEAD(at_time) OVER (
        PARTITION BY
          employee_id
        ORDER BY
          at_time
      ) AS lead_at_time     ,
      LEAD(punch_type) OVER (
        PARTITION BY
          employee_id
        ORDER BY
          at_time
      ) AS lead_punch_type
    FROM
      employee_attendance
  )
SELECT
  employee_id     ,
  at_date         ,
  at_time         ,
  punch_type      ,
  lead_at_date    ,
  lead_at_time    ,
  lead_punch_type ,
  TIME_FORMAT     (
    TIMEDIFF (lead_at_time, at_time),
    "%H:%i:%s"
  ) AS hours
FROM
  employee_attendance_with_lead_result
WHERE
  punch_type = "In"
  AND lead_punch_type = 'Out'
ORDER BY
  employee_id,
  at_date    ,
  at_time
