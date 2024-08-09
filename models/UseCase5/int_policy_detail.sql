{% set source_system = var('source_system', 'EPIC_US') %}

with revenue_fact_base as (
    select
        policy_key,
        bu_key,
        client_key,
        carrier_insurer_key,
        carrier_payee_key,
        product_key,
        product_line_key,
        producer_key,
        client_producer_key,
        client_account_manager_key,
        invoice_date_key,
        bill_type_key,
        billed_premium_amt_lcl,
        billed_premium_amt_usd,
        commission_revenue_amt_lcl,
        commission_revenue_amt_usd,
        fee_revenue_amt_lcl,
        fee_revenue_amt_usd
    from {{ ref('stg_revenue_detail') }}
    where env_source_code = '{{ source_system }}'
),

revenue_fact_main as (
    select
        policy_key,
        bu_key,
        client_key,
        carrier_insurer_key,
        carrier_payee_key,
        product_key,
        product_line_key,
        producer_key as producer01_key,
        client_producer_key,
        client_account_manager_key,
        invoice_date_key,
        bill_type_key,
        sum(billed_premium_amt_lcl) over (partition by policy_key) as billed_premium_amt_lcl,
        sum(billed_premium_amt_usd) over (partition by policy_key) as billed_premium_amt_usd,
        sum(commission_revenue_amt_lcl) over (partition by policy_key) as commission_revenue_amt_lcl,
        sum(commission_revenue_amt_usd) over (partition by policy_key) as commission_revenue_amt_usd,
        sum(fee_revenue_amt_lcl) over (partition by policy_key) as fee_revenue_amt_lcl,
        sum(fee_revenue_amt_usd) over (partition by policy_key) as fee_revenue_amt_usd,
        -- Add other aggregated amount fields as needed
        sum(commission_revenue_amt_usd + fee_revenue_amt_usd) over (partition by policy_key) as revenue_amt_lcl,
        sum(commission_revenue_amt_usd + fee_revenue_amt_usd) over (partition by policy_key, bu_key) as revenue_amt_bu,
        -- Add other partitioned sum calculations
        case
            when sum(billed_premium_amt_lcl) over (partition by policy_key, bu_key) = 0
                 and sum(revenue_amt_lcl) over (partition by policy_key, bu_key) = 0
                then 2
            else 1
        end as priority_order_bu
        -- Add other priority order calculations
    from revenue_fact_base
)

select distinct
    policy_key,
    first_value(bu_key) over (
        partition by policy_key
        order by priority_order_bu, revenue_amt_bu desc nulls last, bu_key
        rows between unbounded preceding and unbounded following
    ) as bu_key,
    -- Add other first_value window functions for other dimensions
    billed_premium_amt_lcl,
    billed_premium_amt_usd,
    commission_revenue_amt_lcl,
    commission_revenue_amt_usd,
    fee_revenue_amt_lcl,
    fee_revenue_amt_usd
    -- Add other amount fields
from revenue_fact_main