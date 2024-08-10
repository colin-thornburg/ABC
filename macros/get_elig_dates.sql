{% macro get_elig_dates(dataset_id) %}
    {% set query %}
        select
            {{ dataset_id }} as datasetid,
            min(m.mo_id) as CurrentEnrollmentStartMoID,
            max(m.mo_id) as CurrentEnrollmentEndMoID,
            min(m2.mo_id) as PriorEnrollmentStartMoID,
            max(m2.mo_id) as PriorEnrollmentEndMoID
        from {{ ref('dataset') }} d
        inner join {{ ref('mo_dim') }} m
            on m.begn_dt between d.currentenrollmentstartdate and d.currentenrollmentenddate
        inner join {{ ref('mo_dim') }} m2
            on m2.begn_dt between d.priorenrollmentstartdate and d.priorenrollmentenddate
        where d.datasetid = {{ dataset_id }}
        limit 1
    {% endset %}

    {% set results = dbt_utils.get_query_results_as_dict(query) %}

    {% if execute %}
        {% do log("Query results for dataset_id " ~ dataset_id ~ ": " ~ results, info=True) %}
        {% if results['DATASETID'] | length == 0 %}
            {% do exceptions.raise_compiler_error("No results found for dataset_id: " ~ dataset_id) %}
        {% endif %}
    {% endif %}

    {{ return(results) }}
{% endmacro %}