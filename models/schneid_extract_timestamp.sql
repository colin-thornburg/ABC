WITH extracted_timestamps AS (
    SELECT
        id,
        person,
        assignment,
        filename,
        load_order,
        TO_TIMESTAMP(SUBSTRING(filename, 35, 14), 'YYYYMMDDHH24MISS') AS file_timestamp
    FROM {{ ref('schneid_source') }}
)

SELECT
    id,
    person,
    assignment,
    filename,
    load_order,
    file_timestamp,
    ROW_NUMBER() OVER (PARTITION BY person ORDER BY file_timestamp ASC) AS file_order
FROM extracted_timestamps

