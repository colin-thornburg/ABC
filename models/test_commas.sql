WITH test_data AS (
    SELECT 12 AS val
    UNION
    SELECT 1234 AS val
    UNION
    SELECT 123456.52 AS val      
),
formatted_data AS (
    {{ add_commas('test_data', 'val', 2) }}
)
SELECT * FROM formatted_data;
