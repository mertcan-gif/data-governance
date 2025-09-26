SELECT
    employee_id,
    first_name,
    last_name,
    email,
    start_date,
    department,
    salary
FROM {{ source('hr', 'employees_raw') }}