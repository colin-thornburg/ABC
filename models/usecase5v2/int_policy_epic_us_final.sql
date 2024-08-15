{{
    config(
        materialized='view'
    )
}}

WITH final_policy AS (
    SELECT
        p.policy_key,
        r.policy_id,
        p.client_key,
        p.effective_date,
        p.expiration_date,
        p.bill_type_key,
        p.bu_id,
        l.policy_status,
        r.renewal_policy_key,
        r.renewal_policy_num,
        COALESCE(rd.agent_commission_amt_usd, 0) AS agent_commission_amt_usd,
        COALESCE(rd.billed_premium_amt_usd, 0) AS billed_premium_amt_usd,
        COALESCE(rd.brokerage_expense_amt_usd, 0) AS brokerage_expense_amt_usd,
        COALESCE(rd.commission_revenue_amt_usd, 0) AS commission_revenue_amt_usd,
        COALESCE(rd.fee_revenue_amt_usd, 0) AS fee_revenue_amt_usd,
    'EPIC_US' AS env_source_code,
    --' var("env_source_code_EPIC_US") }}' AS env_source_code,
    'EPIC_US' AS data_source_code
    FROM {{ ref('int_policy_bu') }} p
    LEFT JOIN {{ ref('int_policy_lifecycle_attr') }} l
        ON p.policy_key = l.policy_key
    LEFT JOIN {{ ref('int_policy_renewal') }} r
        ON p.policy_key = r.policy_key
    LEFT JOIN {{ ref('int_revenue_detail') }} rd
        ON p.policy_key = rd.policy_key
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['policy_id', 'env_source_code', 'data_source_code']) }} as policy_key,
    client_key,
    effective_date,
    expiration_date,
    bill_type_key,
    bu_id,
    policy_status,
    renewal_policy_key,
    renewal_policy_num,
    agent_commission_amt_usd,
    billed_premium_amt_usd,
    brokerage_expense_amt_usd,
    commission_revenue_amt_usd,
    fee_revenue_amt_usd
FROM final_policy