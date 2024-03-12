{% macro create_masking_policy_test_mask(node_database,node_schema) %}

CREATE MASKING POLICY IF NOT EXISTS {{node_database}}.{{node_schema}}.test_mask AS (val string) 
  RETURNS string ->
      CASE WHEN CURRENT_ROLE() IN ('ANALYST') THEN val 
           WHEN CURRENT_ROLE() IN ('TRANSFORMER') THEN SHA2(val)
      ELSE '**********'
      END

{% endmacro %}