WITH policy_base AS (
  SELECT
    a.policy_key,
    a.extract_key,
    IFNULL(TRIM(a.agency_system_policy_id), '') AS policy_id,
    a.office_agency_system_key,
    a.client_key,
    a.broker_key,
    a.insurer_market_key,
    a.payee_market_key,
    a.product_line_key,
    a.producer1_employee_key,
    a.producer2_employee_key,
    a.csr1_employee_key,
    a.department_key,
    a.policy_number AS policy_num,
    a.policy_status,
    NULLIF(TO_DATE(a.effective_date), '1900-01-01 00:00:00.000') AS effective_date,
    NULLIF(TO_DATE(a.expiration_date), '1900-01-01 00:00:00.000') AS expiration_date,
    NULLIF(TO_DATE(a.inception_date), '1900-01-01 00:00:00.000') AS inception_date,
    a.estimated_premium AS estimated_premium_amt,
    a.epic_policy_type_key,
    a.annualized_endorsement_premium AS annualized_endorsement_premium_amt,
    a.written_premium AS written_premium_amt,
    a.annualized_premium AS annualized_premium_amt,
    NULLIF(TO_DATE(a.contracted_expiration_date), '1900-01-01 00:00:00.000') AS contracted_expiration_date,
    b.agency_system_name,
    a.bill_type_key,
    b.agency_system_name || ' - ' || TO_CHAR(b.office_agency_system_key) AS source_system_instance_code
  FROM {{ ref('fdw_s_dim_policy') }} AS a
  INNER JOIN {{ ref('fdw_s_dim_extract') }} AS b
    ON a.extract_key = b.extract_key
  WHERE a.office_agency_system_key = 2
    AND IFNULL(TRIM(a.agency_system_policy_id), '') <> ''	
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      TRIM(a.agency_system_policy_id),
      a.office_agency_system_key
    ORDER BY
      a.extract_key DESC,
      a.policy_key DESC
  ) = 1
),
policy_bu AS (
  SELECT
    a.policy_key,
    c.bu_id,
    TRIM(b.department_code) AS department_code
  FROM policy_base AS a
  LEFT OUTER JOIN {{ ref('fdw_s_dim_department') }} as b
    ON a.department_key = b.department_key
  LEFT OUTER JOIN {{ ref('src_mdm_s_department_to_bu_xref') }} as c
    ON TRIM(b.department_code) = TRIM(c.department_code)
),
employee AS (
  SELECT
    a.employee_key,
    TRIM(a.agency_system_employee_code) AS agency_system_employee_code,
    IFNULL(TRIM(a.employee_name), '') AS employee_name
  FROM {{ ref('fdw_s_dim_employee') }} as a
  INNER JOIN {{ ref('fdw_s_dim_extract') }} as b
    ON a.extract_key = b.extract_key
  WHERE a.employee_key != 0
    AND UPPER(IFNULL(TRIM(a.employee_name), '')) NOT IN ('{ NO EMPLOYEE }', '')
    AND IFNULL(TRIM(a.agency_system_employee_code), '') != ''
),
client AS (
  SELECT
    TRIM(a.agency_system_client_id) AS client_id,
    a.producer1_employee_key,
    a.csr1_employee_key,
    a.client_key,
    IFNULL(TRIM(a.agency_system_client_code), '') AS agency_system_client_code,
    IFNULL(TRIM(a.client_name), '') AS client_name
  FROM {{ ref('fdw_s_dim_client') }} as a
  INNER JOIN {{ ref('fdw_s_dim_extract') }} as b
    ON a.extract_key = b.extract_key
  QUALIFY ROW_NUMBER() OVER (PARTITION BY a.client_key ORDER BY b.extract_key DESC) = 1
),
policy_main AS (
  SELECT
    'env_source_code_EPIC_US' AS env_source_code,
    'Policy' AS data_source_code,
    a.policy_id,
    c.client_id,
    a.client_key,
    CASE
      WHEN mp.market_key IS NOT NULL
        AND IFNULL(UPPER(TRIM(mp.market_name)), '') IN ('{ NO INSURER MARKET }', '{ NO PAYEE MARKET }', '{ NOT A CARRIER }', 'UNMAPPED CARRIER', '{ REQUEST CARRIER TO BE CREATED AS A BSD CODE }')
        THEN NULL
      ELSE TRIM(mp.market_code)
    END AS carrier_payee_id,
    CASE
      WHEN mi.market_key IS NOT NULL
        AND IFNULL(UPPER(TRIM(mi.market_name)), '') IN ('{ NO INSURER MARKET }', '{ NO PAYEE MARKET }', '{ NOT A CARRIER }', 'UNMAPPED CARRIER', '{ REQUEST CARRIER TO BE CREATED AS A BSD CODE }')
        THEN NULL
      ELSE TRIM(mi.market_code)
    END AS carrier_insurer_id,
    CASE
      WHEN pam.employee_key IS NOT NULL
        AND (pam.employee_key = 0 OR UPPER(pam.employee_name) IN ('{ NO EMPLOYEE }', '') OR IFNULL(pam.agency_system_employee_code, '') = '')
        THEN NULL
      ELSE pam.agency_system_employee_code
    END AS account_manager_code,
    CASE
      WHEN pp1.employee_key IS NOT NULL
        AND (pp1.employee_key = 0 OR UPPER(pp1.employee_name) IN ('{ NO EMPLOYEE }', '') OR IFNULL(pp1.agency_system_employee_code, '') = '')
        THEN NULL
      ELSE pp1.agency_system_employee_code
    END AS producer_01_code,
    CASE
      WHEN pp2.employee_key IS NOT NULL
        AND (pp2.employee_key = 0 OR UPPER(pp2.employee_name) IN ('{ NO EMPLOYEE }', '') OR IFNULL(pp2.agency_system_employee_code, '') = '')
        THEN NULL
      ELSE pp2.agency_system_employee_code
    END AS producer_02_code,
    CASE
      WHEN clam.employee_key IS NOT NULL
        AND (clam.employee_key = 0 OR UPPER(clam.employee_name) IN ('{ NO EMPLOYEE }', '') OR IFNULL(clam.agency_system_employee_code, '') = '')
        THEN NULL
      ELSE clam.agency_system_employee_code
    END AS client_account_manager_code,
    CASE
      WHEN clp.employee_key IS NOT NULL
        AND (clp.employee_key = 0 OR UPPER(clp.employee_name) IN ('{ NO EMPLOYEE }', '') OR IFNULL(clp.agency_system_employee_code, '') = '')
        THEN NULL
      ELSE clp.agency_system_employee_code
    END AS client_producer_code,
    CASE
      WHEN eplt.epic_policy_line_type_key IS NOT NULL
        AND IFNULL(TRIM(eplt.policy_line_type_code), '') = ''
        THEN NULL
      ELSE TRIM(eplt.policy_line_type_code)
    END AS product_id,
    CASE
      WHEN pl.product_line_key IS NOT NULL
        AND (a.product_line_key = 0 OR UPPER(TRIM(pl.product_line_code)) IN ('-9999', '000', '', 'NULL'))
        THEN NULL
      ELSE TRIM(pl.product_line_code)
    END AS product_line_id,
    a.effective_date,
    a.expiration_date,
    a.inception_date,
    a.contracted_expiration_date,
    b.bu_id,
    b.department_code,
    TRIM(bt.bill_type_code) AS bill_type_code,
    CAST(IFNULL(a.annualized_endorsement_premium_amt, 0) AS DECIMAL(38,4)) AS annualized_endorsement_premium_amt,
    CAST(IFNULL(a.written_premium_amt, 0) AS DECIMAL(38,4)) AS written_premium_amt,
    CAST(IFNULL(a.annualized_premium_amt, 0) AS DECIMAL(38,4)) AS annualized_premium_amt,
    CAST(IFNULL(a.estimated_premium_amt, 0) AS DECIMAL(38,4)) AS estimated_premium_amt
  FROM policy_base AS a
  LEFT OUTER JOIN {{ ref('fdw_s_dim_market') }} AS mi ON a.insurer_market_key = mi.market_key
  LEFT OUTER JOIN {{ ref('fdw_s_dim_market') }} AS mp ON a.payee_market_key = mp.market_key
  LEFT OUTER JOIN employee AS pam ON a.csr1_employee_key = pam.employee_key
  LEFT OUTER JOIN employee AS pp1 ON a.producer1_employee_key = pp1.employee_key
  LEFT OUTER JOIN employee AS pp2 ON a.producer2_employee_key = pp2.employee_key
  LEFT OUTER JOIN client AS c ON a.client_key = c.client_key
  LEFT OUTER JOIN employee AS clp ON c.producer1_employee_key = clp.employee_key
  LEFT OUTER JOIN employee AS clam ON c.csr1_employee_key = clam.employee_key
  LEFT OUTER JOIN {{ ref('fdw_s_dim_epic_policy_line_type') }} AS eplt ON a.epic_policy_type_key = eplt.epic_policy_line_type_key
    AND a.office_agency_system_key = eplt.office_agency_system_key
  LEFT OUTER JOIN {{ ref('fdw_s_dim_product_line') }} AS pl ON a.product_line_key = pl.product_line_key
  LEFT OUTER JOIN policy_bu AS b ON a.policy_key = b.policy_key
  LEFT OUTER JOIN {{ ref('fdw_s_dim_bill_type') }} AS bt ON a.bill_type_key = bt.bill_type_key
)

SELECT
  CASE 
    WHEN a.policy_id IS NULL THEN 'unknown_key' 
    ELSE TO_VARCHAR(IFNULL(TO_VARCHAR(p.policy_key), '-2')) 
  END AS policy_key,

  CASE 
    WHEN a.client_id IS NULL THEN 'unknown_key' 
    ELSE TO_VARCHAR(IFNULL(TO_VARCHAR(c.client_key), 'invalid_key')) 
  END AS client_key,

  CASE 
    WHEN a.carrier_insurer_id IS NULL THEN 'unknown_key' 
    ELSE TO_VARCHAR(IFNULL(TO_VARCHAR(car1.carrier_key), 'invalid_key')) 
  END AS carrier_insurer_key,
  CASE 
    WHEN a.carrier_payee_id IS NULL THEN 'unknown_key' 
    ELSE TO_VARCHAR(IFNULL(TO_VARCHAR(car1.carrier_key), 'invalid_key')) 
  END AS carrier_payee_key,

  CASE 
    WHEN a.account_manager_code IS NULL THEN 'unknown_key' 
    ELSE TO_VARCHAR(IFNULL(TO_VARCHAR(pam.producer_key), 'invalid_key')) 
  END AS account_manager_key,

  CASE 
    WHEN a.producer_01_code IS NULL THEN 'unknown_key' 
    ELSE TO_VARCHAR(IFNULL(TO_VARCHAR(pp1.producer_key), 'invalid_key')) 
  END AS producer_01_key,

  CASE 
    WHEN a.producer_02_code IS NULL THEN 'unknown_key' 
    ELSE TO_VARCHAR(IFNULL(TO_VARCHAR(pp2.producer_key), 'invalid_key')) 
  END AS producer_02_key,

  CASE 
    WHEN a.client_account_manager_code IS NULL THEN 'unknown_key' 
    ELSE TO_VARCHAR(IFNULL(TO_VARCHAR(cam.producer_key), 'invalid_key')) 
  END AS client_account_manager_key,

  CASE 
    WHEN a.client_producer_code IS NULL THEN 'unknown_key' 
    ELSE TO_VARCHAR(IFNULL(TO_VARCHAR(cp1.producer_key), 'invalid_key')) 
  END AS client_producer_key,

  CASE 
    WHEN a.product_id IS NULL THEN 'unknown_key' 
    ELSE TO_VARCHAR(IFNULL(TO_VARCHAR(prd1.product_key), 'invalid_key')) 
  END AS product_key,

  CASE 
    WHEN a.product_line_id IS NULL THEN 'unknown_key' 
    ELSE TO_VARCHAR(IFNULL(TO_VARCHAR(prd2.product_key), 'invalid_key')) 
  END AS product_line_key,

  CASE WHEN a.effective_date IS NULL THEN '-1'
    ELSE TO_VARCHAR(IFNULL(
      d1.date_key,
      CASE
        WHEN a.effective_date > '2099-12-31' THEN '-3'
        WHEN a.effective_date < '1900-01-01' THEN '-1'
        ELSE '-2'
      END
    ))
  END AS effective_date_key,
  CASE WHEN a.expiration_date IS NULL THEN '-1'
    ELSE TO_VARCHAR(IFNULL(
      d2.date_key,
      CASE
        WHEN a.expiration_date > '2099-12-31' THEN '-3'
        WHEN a.expiration_date < '1900-01-01' THEN '-1'
        ELSE '-2'
      END
    ))
  END AS expiration_date_key,
  CASE WHEN a.inception_date IS NULL THEN '-1'
    ELSE TO_VARCHAR(IFNULL(
      d3.date_key,
      CASE
        WHEN a.inception_date > '2099-12-31' THEN '-3'
        WHEN a.inception_date < '1900-01-01' THEN '-1'
        ELSE '-2'
      END
    ))
  END AS inception_date_key,
  CASE WHEN a.contracted_expiration_date IS NULL THEN '-1'
    ELSE TO_VARCHAR(IFNULL(
      d4.date_key,
      CASE
        WHEN a.contracted_expiration_date > '2099-12-31' THEN '-3'
        WHEN a.contracted_expiration_date < '1900-01-01' THEN '-1'
        ELSE '-2'
      END
    ))
  END AS contracted_expiration_date_key,

CASE 
    WHEN a.env_source_code IS NULL THEN 'unknown_key' 
    ELSE TO_VARCHAR(IFNULL(TO_VARCHAR(ss.source_system_key), 'invalid_key')) 
  END AS source_system_key,
CASE 
    WHEN a.env_source_code IS NULL THEN 'unknown_key' 
    ELSE TO_VARCHAR(IFNULL(TO_VARCHAR(ssi.source_system_instance_key), 'invalid_key')) 
  END AS source_system_instance_key,

  CASE WHEN a.bu_id IS NULL THEN '9005' ELSE TO_VARCHAR(IFNULL(TO_VARCHAR(bu1.bu_key), '-2')) END AS bu_key,

  CASE 
  WHEN a.bu_id IS NULL AND a.department_code IS NULL THEN '9005' 
  ELSE COALESCE(TO_VARCHAR(bu2.bu_key), TO_VARCHAR(bu3.bu_key), '-2') 
END AS bu_department_key,

CASE 
  WHEN a.department_code IS NULL OR a.bu_id IS NULL OR bu1.state_code IS NULL OR bu1.country_name IS NULL THEN '-1' 
  ELSE IFNULL(TO_VARCHAR(cs.country_state_key), '-2') 
END AS bu_state_key,


  CASE 
    WHEN p.bill_type_code IS NULL THEN 'unknown_key' 
    ELSE TO_VARCHAR(IFNULL(TO_VARCHAR(bt.bill_type_key), 'invalid_key')) 
  END AS bill_type_key,
    a.env_source_code,
	a.data_source_code,
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
  bu1.department_code AS bu_department_code,
  bu1.state_code,
  a.effective_date,
  a.expiration_date,
  a.inception_date,
  a.contracted_expiration_date,
  a.annualized_endorsement_premium_amt,
  a.written_premium_amt,
  a.annualized_premium_amt,
  a.estimated_premium_amt
FROM policy_main AS a
LEFT OUTER JOIN {{ ref('edw_d_policy') }} AS p ON p.policy_id = a.policy_id
LEFT OUTER JOIN {{ ref('edw_d_client') }} AS c ON c.client_id = a.client_id
LEFT OUTER JOIN {{ ref('edw_d_carrier') }} AS car1 ON car1.carrier_id = a.carrier_insurer_id
LEFT OUTER JOIN {{ ref('edw_d_carrier') }} AS car2 ON car2.carrier_id = a.carrier_payee_id
LEFT OUTER JOIN {{ ref('edw_d_producer') }} AS pam ON pam.producer_id = a.account_manager_code
LEFT OUTER JOIN {{ ref('edw_d_producer') }} AS pp1 ON pp1.producer_id = a.producer_01_code
LEFT OUTER JOIN {{ ref('edw_d_producer') }} AS pp2 ON pp2.producer_id = a.producer_02_code
LEFT OUTER JOIN {{ ref('edw_d_producer') }} AS cam ON cam.producer_id = a.client_account_manager_code
LEFT OUTER JOIN {{ ref('edw_d_producer') }} AS cp1 ON cp1.producer_id = a.client_producer_code
LEFT OUTER JOIN {{ ref('edw_d_product') }} AS prd1 ON prd1.product_id = a.product_id
LEFT OUTER JOIN {{ ref('edw_d_product') }} AS prd2 ON prd2.product_id = a.product_line_id
LEFT OUTER JOIN {{ ref('edw_d_date') }} AS d1 ON d1.date_value = a.effective_date
LEFT OUTER JOIN {{ ref('edw_d_date') }} AS d2 ON d2.date_value = a.expiration_date
LEFT OUTER JOIN {{ ref('edw_d_date') }} AS d3 ON d3.date_value = a.inception_date
LEFT OUTER JOIN {{ ref('edw_d_date') }} AS d4 ON d4.date_value = a.contracted_expiration_date
LEFT OUTER JOIN {{ ref('edw_d_source_system') }} AS ss ON ss.source_system_code = a.env_source_code
LEFT OUTER JOIN {{ ref('edw_d_source_system_instance') }} AS ssi ON ssi.source_system_instance_code = a.env_source_code
LEFT OUTER JOIN {{ ref('edw_d_bu') }} AS bu1 ON bu1.bu_id = a.bu_id
LEFT OUTER JOIN {{ ref('edw_d_bu') }} AS bu2 ON bu2.bu_id = bu1.department_code
LEFT OUTER JOIN {{ ref('edw_d_bu') }} AS bu3 ON bu3.bu_id = a.department_code
LEFT OUTER JOIN {{ ref('edw_d_country_state') }} AS cs ON cs.country_state_code = bu1.state_code AND cs.country_name = bu1.country_name AND cs.country_state_type_code = 'State Province'
LEFT OUTER JOIN {{ ref('edw_d_bill_type') }} as bt ON bt.bill_type_code = a.bill_type_code
