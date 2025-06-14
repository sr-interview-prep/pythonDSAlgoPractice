WITH attendance_level1 AS (
    SELECT
        ea.employee_id,
        am.employee_name,
        d.department,
        dd.designation,
        (
            at_date :: TIMESTAMP + at_time :: TIME
        ) AS in_date_time,
        punch_type
    FROM
        employee_attendance ea
        INNER JOIN employee_master am
        ON ea.employee_id = am.employee_id
        INNER JOIN department_master d
        ON d.department_id = am.department_id
        INNER JOIN designation_master dd
        ON dd.designation_id = am.designation_id
),
attendance_level2 AS (
    SELECT
        A.employee_id,
        A.employee_name,
        A.department,
        A.designation,
        A.punch_type,
        A.in_date_time,
        LEAD(
            A.punch_type
        ) OVER (
            PARTITION BY A.employee_id
            ORDER BY
                A.in_date_time
        ) AS next_punch_type,
        LEAD(
            A.in_date_time
        ) OVER (
            PARTITION BY A.employee_id
            ORDER BY
                A.in_date_time
        ) AS next_at_datetime
    FROM
        attendance_level1 A
),
attendance_level3 AS (
    SELECT
        employee_id,
        employee_name,
        department,
        designation,
        punch_type AS in_punch_type,
        in_date_time,
        next_punch_type AS out_punch_type,
        next_at_datetime AS out_date_time
    FROM
        attendance_level2
    WHERE
        punch_type = 'In'
        AND next_punch_type = 'Out'
),
attendance_level5 AS (
    SELECT
        employee_id,
        employee_name,
        department,
        designation,
        EXTRACT(EPOCH FROM (out_date_time - in_date_time))::integer AS Seconds,
        in_date_time,
        out_date_time
    FROM
        attendance_level3
)
SELECT
    employee_id,
    employee_name,
    department,
    designation,
    in_date_time,
    out_date_time,
    TO_CHAR(
        (INTERVAL '1 second' * Seconds)::interval,
        'HH24:MI:SS'
    ) AS work_time
FROM
    attendance_level5;
