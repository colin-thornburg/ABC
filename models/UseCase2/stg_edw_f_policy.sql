with source as (
    select * from {{ ref('f_policy') }}
)
select *
from source