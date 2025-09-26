-- This is the final model, ready for reporting.
-- It renames employee_id to user_id for consumption by BI tools.

select
    employee_id as user_id, -- Renaming 'employee_id' to 'user_id'
    first_name,
    last_name,
    employment_status
from
    {{ ref('base_employees') }}