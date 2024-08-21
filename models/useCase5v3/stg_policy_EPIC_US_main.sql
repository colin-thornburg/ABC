-- models/staging/stg_policy_EPIC_US_main.sql

{{ config(materialized='ephemeral') }}

WITH policy_data AS (
    SELECT * FROM {{ ref('stg_policy_EPIC_US_source') }}
),
policy_bu AS (
    SELECT * FROM {{ ref('stg_policy_EPIC_US_bu') }}
),
employee AS (
    SELECT * FROM {{ ref('stg_employee_EPIC_US') }}
),
client AS (
    SELECT * FROM {{ ref('stg_client_EPIC_US') }}
)

SELECT
    a.policy_key,
    a.client_key,
    -- ... (rest of the fields)
    a.policy_id,
    a.client_id,
    a.carrier_insurer_id,
    a.carrier_payee_id,
    a.account_manager_code,
    a.producer_01_code,
    a.producer_02_code,
    a.client_account_manager_code,
    a.client_producer_code,
    a.product_id,
    a.product_line_id,
    a.bill_type_code,
    a.department_code,
    a.bu_id,
    a.bu_department_code,
    a.state_code,
    a.effective_date,
    a.expiration_date,
    a.inception_date,
    a.contracted_expiration_date
FROM policy_data AS a
-- LEFT OUTER JOIN { source('os1_fdw', 's_dim_market') }} AS mi
LEFT OUTER JOIN {{ ref('s_dim_market') }} AS mi
    ON a.insurer_market_key = mi.market_key
-- LEFT OUTER JOIN { source('os1_fdw', 's_dim_market') }} AS mp
LEFT OUTER JOIN {{ ref('s_dim_market') }} AS mp
    ON a.payee_market_key = mp.market_key
LEFT OUTER JOIN employee AS pam
    ON a.csr1_employee_key = pam.employee_key
LEFT OUTER JOIN employee AS pp1
    ON a.producer1_employee_key = pp1.employee_key
LEFT OUTER JOIN employee AS pp2
    ON a.producer2_employee_key = pp2.employee_key
LEFT OUTER JOIN client AS c
    ON a.client_key = c.client_key
LEFT OUTER JOIN employee AS clp
    ON c.producer1_employee_key = clp.employee_key
LEFT OUTER JOIN employee AS clam
    ON c.csr1_employee_key = clam.employee_key
-- LEFT OUTER JOIN { source('os1_fdw', 's_dim_epic_policy_line_type') }} AS eplt
LEFT OUTER JOIN {{ ref('s_dim_epic_policy_line_type') }} AS eplt
    ON a.epic_policy_type_key = eplt.epic_policy_line_type_key
    AND a.office_agency_system_key = eplt.office_agency_system_key
-- LEFT OUTER JOIN { source('os1_fdw', 's_dim_product_line') }} AS pl
LEFT OUTER JOIN {{ ref('s_dim_product_line') }} AS pl
    ON a.product_line_key = pl.product_line_key
LEFT OUTER JOIN policy_bu AS b
    ON a.policy_key = b.policy_key
-- LEFT OUTER JOIN { source('os1_fdw', 's_dim_bill_type') }} AS bt
LEFT OUTER JOIN {{ ref('s_dim_bill_type') }} AS bt
    ON a.bill_type_key = bt.bill_type_key