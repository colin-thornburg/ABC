SELECT a.policy_key,
    CAST(COALESCE(b.renewal_policy_key, -1) AS VARCHAR) AS renewal_policy_key,
    a.client_key,
    a.bu_key,
    a.carrier_payee_key,
    a.product_key,
    a.product_line_key,
    a.effective_date_key,
    a.expiration_date_key,
    a.cancel_date_key,
    a.producer_01_key,
    FALSE AS source_system_purge_ind 
FROM {{ ref('int_edw_f_policy_EPIC_US_base') }} as a
    LEFT OUTER JOIN
    {{ ref('edw_f_policy_renewal_final') }} AS b
    ON a.policy_key = b.policy_key

UNION ALL

SELECT 
    policy_key,
    CAST(COALESCE(renewal_policy_key, -1) AS VARCHAR) AS renewal_policy_key,
    client_key,
    bu_key,
    carrier_payee_key,
    product_key,
    product_line_key,
    effective_date_key,
    expiration_date_key,
    cancel_date_key,
    producer_01_key,
    source_system_purge_ind 
FROM {{ ref('edw_f_policy') }}
WHERE env_source_code = 'FDW'
AND source_system_purge_ind

UNION ALL

SELECT 
    policy_key,
    CAST(COALESCE(renewal_policy_key, -1) AS VARCHAR) AS renewal_policy_key,
    client_key,
    bu_key,
    carrier_payee_key,
    product_key,
    product_line_key,
    effective_date_key,
    expiration_date_key,
    cancel_date_key,
    producer_01_key,
    source_system_purge_ind 
FROM {{ ref('edw_f_policy') }}
WHERE env_source_code IN ('EPIC_US', 'EPIC_CAN', 'AIM_CF', 'FDW')