{% macro generate_sources_yml() %}
    {% set query %}
        SELECT SOURCE_TABLE, SOURCE_SCHEMA
        FROM {{target.database}}.{{target.schema}}.METADATA_TABLE
    {% endset %}

    {% do log(query, info=True) %}

    {% set results = run_query(query).rows %}

    {{ log('Number of rows from metadata_table: ' ~ results | length, info=true) }}

    {% if execute %}
        {% set source_configs = [] %}

        -- Iterating over results to construct the table entries
        {% for row in results %}
            {% set config_entry = '      - name: ' ~ row[0] ~ '\n' %}
            {% do source_configs.append(config_entry) %}
        {% endfor %}

        -- Constructing the full sources.yml content
        {% set full_config = 'version: 2\n\nsources:\n  - name: <source_name_placeholder>\n    database: ' ~ target.database ~ '\n    schema: ' ~ target.schema ~ '\n    tables:\n' ~ (source_configs | join('')) %}

        -- Logging the full content
        {{ log('Complete sources.yml content:\n' ~ full_config, info=true) }}
    {% endif %}
    {{ full_config }}
{% endmacro %}