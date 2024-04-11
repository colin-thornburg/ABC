-- my_dynamic_model.sql

{% set dynamic_sql = generate_simple_dynamic_sql('users', select_key_columns_only=true) %}

-- Now, use the dynamic_sql as part of a larger query, or directly execute it
-- Example of using the result as a part of a larger query:
WITH dynamic_query AS (
    {{ dynamic_sql }}
)

SELECT *
FROM dynamic_query
