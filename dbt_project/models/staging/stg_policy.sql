{{ config(materialized='view') }}

with source_data as (
    select * from {{ source('dagster', 'silver_policy') }}
)

select
    policy_id,
    policy_date,
    tenure_years,
    premium_amount,
    client_id AS policy_holder_id,
    asset_id,
    asset_type,
    asset_value,
    (premium_amount * 100) * (1 + (tenure_years * 0.05)) as derived_coverage_limit,
    policy_holder_name,
    policy_holder_dob,
    policy_holder_city,
    policy_holder_state,
    policy_holder_income,
    policy_holder_marriage_status,
    policy_holder_children,
    policy_holder_beneficiary_name,
    policy_holder_beneficiary_relationship,
    policy_holder_beneficiary_city,
    policy_holder_beneficiary_state,
from source_data