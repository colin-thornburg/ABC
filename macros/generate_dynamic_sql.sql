{% macro generate_simple_dynamic_sql(table_name, select_key_columns_only=false) %}

    {% if select_key_columns_only %}
        {% set query %}
        SELECT column_name
        FROM {{ ref('metadata_table') }}
        WHERE table_name = '{{ table_name }}'
        AND is_key_column = 1
        {% endset %}
    {% else %}
        {% set query %}
        SELECT column_name
        FROM {{ ref('metadata_table') }}
        WHERE table_name = '{{ table_name }}'
        {% endset %}
    {% endif %}

    {{ return(query) }}

{% endmacro %}