with source_data as (
    select * from {{ ref('stg_policy') }}
)

select
    policy_id,
    policy_date,
    tenure_years,
    premium_amount,
    policy_holder_id,
    asset_id,
    derived_coverage_limit
from source_data