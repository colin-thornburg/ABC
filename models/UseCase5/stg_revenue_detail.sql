{% set source_system = var('source_system', 'EPIC_US') %}

with source as (
    select * from {{ ref('revenue_detail_' ~ source_system) }}
),

staged as (
    select
        '{{ source_system }}' as source_system,
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
        fee_revenue_amt_usd,
        env_source_code
    from source
)

select * from staged