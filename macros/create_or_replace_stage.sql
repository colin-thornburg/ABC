{# 
    To run this macro as an operation as a dry run, just use the following command:
        dbt run-operation create_or_replace_stage

    To fully execute this macro and run the commands in snowflake, use the following command instead:
        dbt run-operation create_or_replace_stage --args '{dry_run: False}'

    Read more about running macros as operations here:
        https://docs.getdbt.com/reference/commands/run-operation/

    Args:
        - dry_run -- Default to True - dry run will output the SQL to the logs but won't execute any of it.
#}


{% macro create_or_replace_stage(dry_run=True) %}

    {# Configure here. You could modify this macro to make these params if you like. #}
    {% set stage_db     = 'colint_dev' %}
    {% set stage_schema = 'dbt_cthornburg' %}
    {% set stage_name   = 'adls_blob_stage' %}
    {% set azure_url    = 'azure://mydbtstorageacct.blob.core.windows.net/mydbtcontainer' %}
    {% set sf_role      = 'transformer' %}

    {% set create_or_replace_query %}
        USE ROLE {{sf_role}};

        -- create the stage
        create or replace stage {{stage_db}}.{{stage_schema}}.{{stage_name}} 
        url='{{azure_url}}'
        FILE_FORMAT= (
            TYPE='PARQUET'
        );
    {% endset %}

    {% if dry_run %}
        {% do log('adls stage creation dry run :\n' ~ create_or_replace_query, True) %}
    {% else %}
        {% do run_query(create_or_replace_query) %}
    {% endif %}

    {# Always a good idea to explicitly jump back to the default role for the current environment #}
    {% do run_query('USE ROLE ' ~ target.role) %}
{% endmacro %}