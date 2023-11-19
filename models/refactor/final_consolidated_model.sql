-- final_consolidated_model.sql
SELECT
    *
FROM {{ ref('int_level3_final_transformations') }}
-- This model is where you do the final shaping of the data, including selecting specific columns, renaming, etc.
