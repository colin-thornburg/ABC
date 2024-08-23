Select
    extract_key,
    extract_name,
    extract_date,
    agency_system_name,
    bill_type_key,
    source_system_instance_code
From {{ ref('s_dim_extract') }}
