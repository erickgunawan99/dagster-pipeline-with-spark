-- depends_on: {{ ref('stg_claim') }}
{{ config(materialized='view') }}

with claims as (
    select * from {{ ref('stg_claim') }}
),
policies as (
    select * from {{ ref('dim_policy') }}
)

select
    c.claim_id,
    c.claim_policy_id,
    p.policy_holder_id,
    p.asset_id,
    c.payout,
    c.claim_date,
    p.derived_coverage_limit,
    
    -- COMPARE PAYOUT TO LIMIT
    case 
        when c.payout > p.derived_coverage_limit then 'over_limit'
        when c.payout > (p.derived_coverage_limit * 0.8) then 'warning_near_limit'
        else 'within_limit'
    end as limit_status

from claims c
left join policies p on c.claim_policy_id = p.policy_id