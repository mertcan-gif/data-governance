-- This model applies basic business logic.

select
    employee_id,
    split_part(full_name, ' ', 1) as first_name, -- Splitting the name
    split_part(full_name, ' ', 2) as last_name,
    employment_status
from
    {{ ref('stg_employees') }}
where
    employment_status in ('Active', 'On Leave')