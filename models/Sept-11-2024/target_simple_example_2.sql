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
    ],
    pre_hook=delete_expired_policies()
) }}

{% if is_incremental() %}
    {% set target_relation = adapter.get_relation(this.database, this.schema, this.table) %}
    {% if target_relation is none %}
        -- Target table doesn't exist; perform full load
        WITH source_data AS (
            SELECT 
                policy_key,
                client_key,
                product_key,
                effective_date_key,
                expiration_date_key,
                written_premium_amt,
                annualized_premium_amt,
                commission_revenue_amt_usd,
                fee_revenue_amt_usd
            FROM {{ ref('temp_edw_f_policy_EPIC_US_FINAL') }}
            WHERE insert_to_fact
        )
    {% else %}
        -- Target table exists; perform incremental logic
        WITH source_data AS (
            SELECT 
                s.policy_key,
                s.client_key,
                s.product_key,
                s.effective_date_key,
                s.expiration_date_key,
                s.written_premium_amt,
                s.annualized_premium_amt,
                s.commission_revenue_amt_usd,
                s.fee_revenue_amt_usd
            FROM {{ ref('temp_edw_f_policy_EPIC_US_FINAL') }} AS s
            LEFT JOIN {{ this }} AS t
                ON s.policy_key = t.policy_key
            WHERE s.insert_to_fact
              AND (
                  s.client_key <> t.client_key OR
                  s.product_key <> t.product_key OR
                  s.effective_date_key <> t.effective_date_key OR
                  s.expiration_date_key <> t.expiration_date_key OR
                  s.written_premium_amt <> t.written_premium_amt OR
                  s.annualized_premium_amt <> t.annualized_premium_amt OR
                  s.commission_revenue_amt_usd <> t.commission_revenue_amt_usd OR
                  s.fee_revenue_amt_usd <> t.fee_revenue_amt_usd OR
                  t.policy_key IS NULL  -- Include new records
              )
        )
    {% endif %}
{% else %}
    -- Full refresh
    WITH source_data AS (
        SELECT 
            policy_key,
            client_key,
            product_key,
            effective_date_key,
            expiration_date_key,
            written_premium_amt,
            annualized_premium_amt,
            commission_revenue_amt_usd,
            fee_revenue_amt_usd
        FROM {{ ref('temp_edw_f_policy_EPIC_US_FINAL') }}
        WHERE insert_to_fact
    )
{% endif %}

SELECT * FROM source_data
