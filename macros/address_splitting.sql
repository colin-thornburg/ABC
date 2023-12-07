{% macro split_address(address) %}
    split_part({{ address }}, ', ', 1) AS PatientStreet,
    split_part({{ address }}, ', ', 2) AS PatientCity,
    split_part({{ address }}, ', ', 3) AS PatientState,
    split_part({{ address }}, ', ', 4) AS PatientZip
{% endmacro %}