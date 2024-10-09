{{ config(
    materialized='incremental',
    unique_key='policy_key',
    on_schema_change='ignore',
    incremental_strategy='merge',
    merge_update_columns=[
        'client_key',
        'product_key',
        'effective_date_key',
        'expiration_date_key',
        'written_premium_amt',
        'annualized_premium_amt',
        'commission_revenue_amt_usd',
        'fee_revenue_amt_usd'
    ]
) }}

SELECT 
    policy_key,
    client_key,
    product_key,
    effective_date_key,
    expiration_date_key,
    1 as written_premium_amt,

    commission_revenue_amt_usd,
    'some data' as new_column,
    fee_revenue_amt_usd
FROM {{ ref('temp_edw_f_policy_EPIC_US_FINAL') }}
WHERE insert_to_fact
