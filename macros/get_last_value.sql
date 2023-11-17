{% macro get_last_value(column_name, relation) %}
  
{% set relation_query %}
select distinct({{ column_name }})
from {{ relation }}
order by 1 desc
limit 1
{% endset %}
  
{% set results = run_query(relation_query) %}
  
{% if execute %}
{# Return the first column's first value #}
{% set last_value = results.columns[0].values()[0] %}
{% else %}
{% set last_value = None %}
{% endif %}
  
{{ return(last_value) }}
  
{% endmacro %}