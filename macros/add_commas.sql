{% macro add_commas(table_name, column_name, round_prec) %}

WITH prep AS (
    SELECT 
    TO_VARIANT(ROUND({{ column_name }}, {{ round_prec }}))::STRING AS str_val
    FROM {{ table_name }}
),
reversed AS (
    SELECT REVERSE(str_val) as rev_val
    FROM prep
),
formatted AS (
    SELECT 
    CASE 
        WHEN LENGTH(rev_val) > 3 THEN 
            INSERT(rev_val, 4, 0, ',')
        ELSE rev_val
    END as rev_val
    FROM reversed
),
formatted2 AS (
    SELECT 
    CASE 
        WHEN LENGTH(rev_val) > 7 THEN 
            INSERT(rev_val, 8, 0, ',')
        ELSE rev_val
    END as rev_val
    FROM formatted
),
formatted3 AS (
    SELECT 
    CASE 
        WHEN LENGTH(rev_val) > 11 THEN 
            INSERT(rev_val, 12, 0, ',')
        ELSE rev_val
    END as rev_val
    FROM formatted2
)
SELECT REVERSE(rev_val) as formatted_val FROM formatted3

{% endmacro %}
