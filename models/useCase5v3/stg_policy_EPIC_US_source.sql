-- models/staging/stg_policy_EPIC_US_source.sql

{{ config(materialized='ephemeral') }}

WITH policy_base AS (
    SELECT
        p.policy_key,
        p.extract_key,
        IFNULL(TRIM(p.agency_system_policy_id), '') AS policy_id,
        p.office_agency_system_key,
        p.client_key,
        p.broker_key,
        p.insurer_market_key,
        p.payee_market_key,
        p.product_line_key,
        p.producer1_employee_key,
        p.producer2_employee_key,
        p.csr1_employee_key,
        p.department_key,
        p.policy_number AS policy_num,
        p.policy_status,
        NULLIF(TO_DATE(p.effective_date), '1900-01-01') AS effective_date,
        NULLIF(TO_DATE(p.expiration_date), '1900-01-01') AS expiration_date,
        NULLIF(TO_DATE(p.inception_date), '1900-01-01') AS inception_date,
        p.estimated_premium AS estimated_premium_amt,
        p.epic_policy_type_key,
        p.annualized_endorsement_premium AS annualized_endorsement_premium_amt,
        p.written_premium AS written_premium_amt,
        p.annualized_premium AS annualized_premium_amt,
        NULLIF(TO_DATE(p.contracted_expiration_date), '1900-01-01') AS contracted_expiration_date,
        e.agency_system_name,
        p.bill_type_key,
        e.agency_system_name || ' - ' || TO_CHAR(e.office_agency_system_key) AS source_system_instance_code
    --FROM { source('os1_fdw', 's_dim_policy') }} AS p
    FROM {{ ref('s_dim_policy_UC5') }} AS p
    -- INNER JOIN { source('os1_fdw', 's_dim_extract') }} AS e
    INNER JOIN {{ ref('s_dim_extract') }} AS e
        ON p.extract_key = e.extract_key
    WHERE p.office_agency_system_key = 1234 -- Hardcoded, replace with actual EPIC_US OAS ID
      AND IFNULL(TRIM(p.agency_system_policy_id), '') <> ''
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY TRIM(p.agency_system_policy_id), p.office_agency_system_key
        ORDER BY p.extract_key DESC, p.policy_key DESC
    ) = 1
)

SELECT * FROM policy_base