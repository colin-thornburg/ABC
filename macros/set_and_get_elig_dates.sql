{% macro set_and_get_elig_dates(dataset_id) %}
  {% if not execute %}
    {{ return({}) }}
  {% endif %}

  {% if var('elig_dates') is mapping and var('elig_dates').get(dataset_id) %}
    {{ return(var('elig_dates').get(dataset_id)) }}
  {% else %}
    {% set elig_dates = get_elig_dates(dataset_id) %}
    {% do var('elig_dates').update({dataset_id: elig_dates}) %}
    {{ return(elig_dates) }}
  {% endif %}
{% endmacro %}