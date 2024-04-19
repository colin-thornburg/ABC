{% macro yo_yo(dry_run=True) %}

  -- Constructing the SQL query to fetch the current timestamp
  {% set query %}
    SELECT current_timestamp()
  {% endset %}

  -- Logging the constructed query for transparency
  {% do log(query, info=True) %}

  -- Conditionally executing the query based on dry_run value
  {% if dry_run %}
    -- If dry_run is True, just log the query without executing
    {{ log('Dry run mode: Query not executed. Query is: ' ~ query, info=True) }}
  {% else %}
    -- If dry_run is False, execute the query and log the result
    {% set results = run_query(query).rows %}
    {{ log('Query executed. Current timestamp: ' ~ results[0][0], info=True) }}
  {% endif %}

{% endmacro %}