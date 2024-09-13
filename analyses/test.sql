--claim_type_month.sql

with months as (
    select distinct
        extract(year from claim_date) as year,
        extract(month from claim_date) as month
    from {{ ref('claims_data') }}
),

claim_types as (
    select distinct claim_type from {{ ref('claims_data') }}
),

expected_combinations as (
    select
        ct.claim_type,
        m.year,
        m.month
    from claim_types ct
    cross join months m
),

actual_combinations as (
    select distinct
        claim_type,
        extract(year from claim_date) as year,
        extract(month from claim_date) as month
    from {{ ref('claims_data') }}
),

missing_combinations as (
    select
        ec.claim_type,
        ec.year,
        ec.month
    from expected_combinations ec
    left join actual_combinations ac
        on ec.claim_type = ac.claim_type
        and ec.year = ac.year
        and ec.month = ac.month
    where ac.claim_type is null
)

select *
from missing_combinations