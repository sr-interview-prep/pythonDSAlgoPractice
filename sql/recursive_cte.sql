"""
There is a table with candidates and their joining date,
Another table with the holiday dates. I need you to give me a sql query to find:
 - The earliest joining dates for a given candidate, and the joining date cannot be on a holiday
 - If the joining date is on a holiday increment it by 1 until its not on a holiday
"""
WITH RECURSIVE adjusted_dates AS (
  SELECT id, name, joining_date
  FROM candidates

  UNION ALL

  SELECT a.id, a.name, a.joining_date + INTERVAL '1 day'
  FROM adjusted_dates a
  JOIN holidays h ON a.joining_date = h.holiday_date
)
SELECT id, name, MIN(joining_date) AS final_joining_date
FROM adjusted_dates
WHERE joining_date NOT IN (SELECT holiday_date FROM holidays)
GROUP BY id, name;
