-- This model cleans up the raw source data.

select
    employee_id,
    full_name,
    status as employment_status -- Renaming 'status' to 'employment_status'
from
    {{ source('successfactors_source', 'raw_employees') }}
