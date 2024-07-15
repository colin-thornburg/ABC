{% macro generate_listagg_sql(table_name) %}
    -- Query the metadata to get columns for the given table where change tracking is enabled
    {% set query_result = run_query("SELECT column_name FROM " ~ ref('metadata_table') ~ " WHERE table_name = '" ~ table_name ~ "' AND change_tracking_enabled = 1 ORDER BY column_name LIMIT 1") %}
    
    -- Check if the query returns results
    {% if query_result and query_result.rows | length > 0 %}
        {% set column_name = query_result.rows[0][0] %}
        -- Concatenate the SQL string
        {% set listagg_sql = "SELECT LISTAGG(" ~ column_name ~ ", ', ') WITHIN GROUP (ORDER BY " ~ column_name ~ ") AS Concatenated_Columns FROM " ~ table_name %}
        {{ return(listagg_sql) }}
    {% else %}
        {{ return("SELECT 'No columns with change tracking enabled' AS Error") }}
    {% endif %}
{% endmacro %}

