{{ config(materialized='view') }}

with source_data as (
    select * from {{ source('dagster', 'silver_claim') }}
)

select
    claim_id,
    claim_policy_id,
    payout,
    claim_event_date AS claim_date
from source_data
  