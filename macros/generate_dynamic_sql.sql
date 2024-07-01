{% macro generate_select_sql(table_name) %}
    -- Initialize an empty list to store column names
    {% set select_columns = [] %}
    
    -- Query the metadata to get columns for the given table where change tracking is enabled
    {% set query_result = run_query("SELECT column_name FROM " ~ ref('metadata_table') ~ " WHERE table_name = '" ~ table_name ~ "' AND change_tracking_enabled = 1") %}
    
    -- Check if the query returns results and append column names to the list
    {% if query_result %}
        {% for row in query_result.rows %}
            {{ select_columns.append(row[0]) }}
        {% endfor %}
    {% endif %}
    
    -- Join the list of column names into a single string separated by commas
    {% set column_list = select_columns | join(', ') %}
    
    -- Return the final SELECT statement
    {{ return("SELECT " ~ column_list ~ " FROM " ~ table_name) }}
{% endmacro %}
