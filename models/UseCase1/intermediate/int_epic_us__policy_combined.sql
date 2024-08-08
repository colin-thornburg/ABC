WITH policy_line_program AS (
    SELECT DISTINCT
        pl.policy_key,
        TRIM(ep.program) AS program,
        TRIM(ep.product) AS product
    FROM {{ ref('stg_epic_us__dim_line') }} pl
    INNER JOIN {{ ref('stg_epic_us__dim_epic_program') }} ep
        ON pl.epic_program_key = ep.epic_program_key
    WHERE COALESCE(UPPER(TRIM(ep.product)), '') NOT IN ('', 'DO NOT USE')
),

policy_product AS (
    SELECT
        policy_key,
        program,
        product,
        COUNT(*) OVER (PARTITION BY policy_key) AS policy_program_product_distinct_cnt,
        COUNT(CASE WHEN UPPER(program) = 'CLIENT ADVANTAGE PRODUCTS' THEN 1 ELSE NULL END) OVER (PARTITION BY policy_key) AS advantage_program_cnt
    FROM policy_line_program
),

policy_product_final AS (
    SELECT
        policy_key,
        program,
        product
    FROM policy_product
    WHERE
        policy_program_product_distinct_cnt = 1
        OR (
            policy_program_product_distinct_cnt > 1
            AND advantage_program_cnt = 1
            AND UPPER(program) = 'CLIENT ADVANTAGE PRODUCTS'
        )
),

policy_line_type AS (
    SELECT
        pl.policy_key,
        eplt.policy_line_type_code,
        eplt.policy_line_type_description
    FROM {{ ref('stg_epic_us__dim_line') }} pl
    LEFT JOIN {{ ref('stg_epic_us__dim_epic_policy_line_type') }} eplt
        ON pl.agency_system_policy_type = eplt.policy_line_type_code
),

combined_data AS (
    SELECT
        p.policy_key,
        p.policy_id,
        p.policy_number,
        p.effective_date,
        p.expiration_date,
        p.inception_date,
        p.policy_status,
        p.bill_type,
        p.written_premium,
        p.annualized_premium,
        p.producer_employee_key,
        p.csr_employee_key,
        p.is_active,
        p.original_policy_id,
        p.office_agency_system_key,
        p.extract_key,
        p.is_deleted,
        p.contracted_expiration_date,
        p.cancellation_reason,
        p.cancellation_reason_other,
        pp.program,
        pp.product AS policy_level_source_code,
        plt.policy_line_type_code,
        plt.policy_line_type_description,
        e1.employee_name AS producer_name,
        e2.employee_name AS account_manager_name,
        ppi.program_eligibility_code,
        ppi.program_eligibility_name,
        ppi.program_desc_01,
        ppi.program_desc_02,
        CASE 
            WHEN UPPER(TRIM(plt.policy_line_type_code)) IN ('CUMB', 'LXS') 
                OR UPPER(TRIM(pp.program)) = 'CLIENT ADVANTAGE PRODUCTS'
                OR UPPER(TRIM(plt.policy_line_type_code)) IN ('1_BR1', 'IBR', 'BRI', '1_BRI')
                OR UPPER(TRIM(plt.policy_line_type_code)) IN ('LENV', 'LUST')
                OR UPPER(TRIM(plt.policy_line_type_code)) IN ('PRBM')
            THEN 'Client Advantage Products'
            ELSE pp.program
        END AS final_program,
        CASE 
            WHEN UPPER(TRIM(plt.policy_line_type_code)) IN ('CUMB', 'LXS') THEN 'Umbrella Advantage'
            WHEN UPPER(TRIM(pp.program)) = 'CLIENT ADVANTAGE PRODUCTS' THEN 'MLP Direct'
            WHEN UPPER(TRIM(plt.policy_line_type_code)) IN ('1_BR1', 'IBR', 'BRI', '1_BRI') THEN 'Builders Risk'
            WHEN UPPER(TRIM(plt.policy_line_type_code)) IN ('LENV', 'LUST') THEN 'Contractors Pollution Protect'
            WHEN UPPER(TRIM(plt.policy_line_type_code)) IN ('PRBM') THEN 'Equipment Breakdown'
            ELSE pp.product
        END AS final_product
    FROM {{ ref('stg_epic_us__dim_policy') }} p
    LEFT JOIN policy_product_final pp ON p.policy_key = pp.policy_key
    LEFT JOIN policy_line_type plt ON p.policy_key = plt.policy_key
    LEFT JOIN {{ ref('stg_epic_us__dim_employee') }} e1 ON p.producer_employee_key = e1.employee_key
    LEFT JOIN {{ ref('stg_epic_us__dim_employee') }} e2 ON p.csr_employee_key = e2.employee_key
    LEFT JOIN {{ ref('stg_mdm__policy_program_input') }} ppi ON p.policy_id = ppi.policy_id
)

SELECT 
    *,
    CASE 
        WHEN DATEDIFF(DAY, effective_date, expiration_date) > 400 THEN TRUE 
        ELSE FALSE 
    END AS multi_year_ind,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM combined_data