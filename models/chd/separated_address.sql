WITH separated_address_data AS (
    SELECT
        *,
        -- Use the macro to split the address
        {{ split_address('PatientAddress') }} 
    FROM {{ ref('stg_pts_members') }} -- Reference the 'stg_pts_members' model
)
SELECT
    *
    -- Additional staging transformations if needed
FROM separated_address_data