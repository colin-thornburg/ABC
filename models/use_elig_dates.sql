{% set elig_dates = get_elig_dates(dataset_id=123) %}

{% if execute %}
    {% do log("Elig Dates: " ~ elig_dates, info=True) %}
{% endif %}

    SELECT
        {{ elig_dates['DATASETID'][0] }} as dataset_id,
        {{ elig_dates['CURRENTENROLLMENTSTARTMOID'][0] }} as current_enrollment_start_mo_id,
        {{ elig_dates['CURRENTENROLLMENTENDMOID'][0] }} as current_enrollment_end_mo_id,
        {{ elig_dates['PRIORENROLLMENTSTARTMOID'][0] }} as prior_enrollment_start_mo_id,
        {{ elig_dates['PRIORENROLLMENTENDMOID'][0] }} as prior_enrollment_end_mo_id
