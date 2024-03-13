{% macro create_masking_policy_temp(node_database, node_schema) %}

CREATE MASKING POLICY IF NOT EXISTS {{ node_database }}.{{ node_schema }}.temp AS (val STRING)
RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('SYSADMIN') THEN val
        ELSE '***********'
    END

{% endmacro %}