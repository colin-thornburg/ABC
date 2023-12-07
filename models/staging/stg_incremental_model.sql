{{
    config(
        materialized='incremental',
        on_schema_change='append_new_columns'
    )
}}

select
    *
from {{ ref('dummy_source') }}


{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses >= to include records arriving later on the same day as the last run of this model)
  where timestamp >= (select max(timestamp) from {{ this }})

{% endif %}