{# dbt run-operation create_share_sql --args '{"share_name": "my_share_name", "account_id": "my_account_id"} #}

{% macro create_share_sql(share_name, account_id) %}
  {% set sql_statements = [
    "use role transformer;",
    "create share " ~ share_name ~ ";",
    "grant usage on database analytics to share " ~ share_name ~ ";",
    "grant reference_usage on database raw to share " ~ share_name ~ ";",
    "alter share " ~ share_name ~ " add accounts = " ~ account_id ~ ";"
  ] %}

  {% for sql in sql_statements %}
    {{ log(sql, info=True) }}
  {% endfor %}
{% endmacro %}