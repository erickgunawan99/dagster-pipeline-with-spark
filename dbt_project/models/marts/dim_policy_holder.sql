with source_data as (
    select * from {{ ref('stg_policy') }}
)

select
    policy_holder_id,
    policy_holder_name,
    policy_holder_dob,
    policy_holder_city,
    policy_holder_state,
    policy_holder_income,
    policy_holder_marriage_status,
    policy_holder_children
from source_data