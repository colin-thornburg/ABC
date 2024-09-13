with base as (
    select
        claim_date,
        extract(month from claim_date) as month,
        extract(year from claim_date) as year,
        claim_type,
        claim_amount
    from {{ ref('claims_data') }}
)

select
    year,
    month,
    claim_type,
    sum(claim_amount) as total_claims
from base
group by year, month, claim_type
order by year, month, claim_type
