WITH policy_line_program AS (
    SELECT DISTINCT
        policy_key,
        TRIM(program) AS program,
        TRIM(product) AS product
    FROM {{ ref('policy_product') }}
    WHERE IFNULL(UPPER(TRIM(product)), '') NOT IN ('', 'DO NOT USE')
),
policy_product AS (
    SELECT
        policy_key,
        program,
        product,
        COUNT(*) OVER (PARTITION BY policy_key) AS policy_program_product_distinct_cnt,
        COUNT(CASE WHEN UPPER(program) = 'CLIENT ADVANTAGE PRODUCTS' THEN 1 ELSE NULL END) OVER (PARTITION BY policy_key) AS advantage_program_cnt
    FROM policy_line_program
)
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