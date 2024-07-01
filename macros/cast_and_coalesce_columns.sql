{% macro cast_and_coalesce_columns(table_name) %}
SELECT
    CAST(item_id AS BIGINT) AS item_id,
    CAST(TRIM(iline) AS VARCHAR(16)) as item_num,
    CAST(TRIM(iitem) AS VARCHAR(16)) AS iitem,
    COALESCE(description, 'No Description') AS description,
    CAST(load_date AS DATE) AS load_date
FROM {{ table_name }}
{% endmacro %}