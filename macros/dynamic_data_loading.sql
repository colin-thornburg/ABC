{% macro dynamic_data_loading(dry_run=True) %}

  -- Constructing the SQL query to fetch metadata
  {% set query %}
    SELECT SOURCE_TABLE, SOURCE_SCHEMA, TARGET_TABLE, TARGET_SCHEMA
    FROM {{target.database}}.{{target.schema}}.METADATA_TABLE
  {% endset %}

  -- Logging the constructed query for transparency
  {% do log(query, info=True) %}

  -- Executing the query to fetch metadata
  {% set results = run_query(query).rows %}
  {{ log('Number of rows from metadata_table: ' ~ results | length, info=true) }}

  -- Initialize a variable to hold the dynamically generated SQL commands
  {% set sql_commands = [] %}

  -- Iterating over results to construct the SQL for data loading
  {% for row in results %}
    {% set load_sql %}
      INSERT INTO {{ row[3] }}.{{ row[2] }} -- TARGET_SCHEMA.TARGET_TABLE
      SELECT * FROM {{ row[1] }}.{{ row[0] }}; -- SOURCE_SCHEMA.SOURCE_TABLE
    {% endset %}
    {% do sql_commands.append(load_sql) %}
  {% endfor %}

  -- Logging and executing the dynamically generated SQL commands
  {% for sql_command in sql_commands %}
    {% do log(sql_command, info=True) %}
    {% if dry_run == false %}
        {% do run_query(sql_command) %}
        {% do log('Data loaded successfully.', info=True) %}
    {% endif %}
  {% endfor %}

{% endmacro %}