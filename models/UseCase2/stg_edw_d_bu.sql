with source as (
    select * from {{ ref('d_bu') }}
)
select *
from source