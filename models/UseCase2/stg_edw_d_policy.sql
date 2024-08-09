with source as (
    select * from {{ ref('d_policy') }}
)
select *
from source