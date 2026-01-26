-- depends_on: {{ ref('stg_policy') }}
with source_data as (
    select * from {{ ref('stg_policy') }}
)

select
    policy_holder_id,
    policy_holder_beneficiary_name as beneficiary_name,
    policy_holder_beneficiary_relationship as beneficiary_relationship,
    policy_holder_beneficiary_city as beneficiary_city,
    policy_holder_beneficiary_state as beneficiary_state
from source_data
where beneficiary_name <> 'n/a'