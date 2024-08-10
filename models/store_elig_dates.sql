-- models/store_elig_dates.sql
{% set elig_dates = get_elig_dates(123) %}

{% do log(elig_dates, info=True) %}

