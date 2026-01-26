with source_data as (
    select * from {{ ref('stg_policy') }}
)

select
    asset_id,
    asset_type,
    asset_value
from source_data