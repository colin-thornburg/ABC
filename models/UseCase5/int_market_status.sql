{{ config(materialized='view') }}

WITH policy_base AS (
    SELECT * FROM {{ ref('seed_policy_base') }}
),

d_carrier AS (
    SELECT * FROM {{ ref('edw_d_carrier') }}
)

SELECT
    pb.policy_key,
    CASE
        WHEN pb.policy_key = pb.policy_key THEN 'New Policy'
        WHEN LAG(dc.carrier_parent_payee_market_type_name) OVER (ORDER BY pb.policy_key) = dc.carrier_parent_payee_market_type_name THEN 'Renewal from same Market'
        ELSE 'New from another Market'
    END AS market_status_code,
    LAG(pb.carrier_payee_key) OVER (ORDER BY pb.policy_key) AS prior_market_key,
    LEAD(pb.carrier_payee_key) OVER (ORDER BY pb.policy_key) AS future_market_key,
    CASE
        WHEN dc.carrier_parent_payee_market_type_name = 'Intermediary' AND dc.carrier_parent_intermediary_market_type_name = 'Gallagher Entity' THEN 'Gallagher Intermediary'
        WHEN dc.carrier_parent_payee_market_type_name = 'Carrier' AND dc.carrier_parent_tier_name = 'Tier 1' THEN 'Tier 1 Carrier'
        WHEN dc.carrier_parent_payee_market_type_name = 'Carrier' AND dc.carrier_parent_tier_name != 'Tier 1' THEN 'Carrier - non Tier 1'
        ELSE 'External Intermediary / Other'
    END AS prior_market_type_code,
    CASE
        WHEN LEAD(dc.carrier_parent_payee_market_type_name) OVER (ORDER BY pb.policy_key) = 'Intermediary' 
             AND LEAD(dc.carrier_parent_intermediary_market_type_name) OVER (ORDER BY pb.policy_key) = 'Gallagher Entity' THEN 'Gallagher Intermediary'
        WHEN LEAD(dc.carrier_parent_payee_market_type_name) OVER (ORDER BY pb.policy_key) = 'Carrier' 
             AND LEAD(dc.carrier_parent_tier_name) OVER (ORDER BY pb.policy_key) = 'Tier 1' THEN 'Tier 1 Carrier'
        WHEN LEAD(dc.carrier_parent_payee_market_type_name) OVER (ORDER BY pb.policy_key) = 'Carrier' 
             AND LEAD(dc.carrier_parent_tier_name) OVER (ORDER BY pb.policy_key) != 'Tier 1' THEN 'Carrier - non Tier 1'
        ELSE 'External Intermediary / Other'
    END AS future_market_type_code
FROM policy_base pb
LEFT JOIN d_carrier dc ON pb.carrier_payee_key = dc.carrier_key