-- int_level2_combined_join.sql
SELECT
    combined.*,
    payment_terms.name_s AS PAYMENT_TERM_DESCRIPTION
FROM {{ ref('int_level1_invoice_lineitem_header_join') }} AS combined
LEFT JOIN {{ ref('stg_live_dim_payment_terms') }} AS payment_terms
ON combined.PAY_TERM = payment_terms.TERM_ID_s
-- Add any additional join logic or filters as needed
