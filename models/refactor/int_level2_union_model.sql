-- int_level2_union_model.sql
SELECT
    *
FROM {{ ref('int_level1_invoice_lineitem_header_join') }}
UNION ALL
SELECT
    *
FROM {{ ref('int_level2_combined_join') }}
-- Adjust columns and logic to match the structure and requirements of the union from your original script
