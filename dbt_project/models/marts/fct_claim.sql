with source_data as (
    select * from {{ ref('int_claim_details') }}
)

select
    claim_id,
    claim_date,
    claim_policy_id AS policy_key,
    policy_holder_id,
    asset_id,
    payout,
    limit_status
from source_data