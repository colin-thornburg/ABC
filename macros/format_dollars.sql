
{% macro format_dollars(column_name, precision=2) %}
    ({{ column_name }} * 3)::numeric(16, {{ precision }})
{% endmacro %}

{# Wrapper macro to test
{% macro format_dollars(column_name, precision=2) %}
  {% set formatted_sql = "(" ~ column_name ~ " * 3)::numeric(16, " ~ precision ~ ")" %}
  {{ log("Generated SQL: " ~ formatted_sql, info=True) }}
{% endmacro %}
#}