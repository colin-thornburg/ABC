WITH OrderedData AS (
    SELECT
        Column_A,
        Column_B,
        LAG(Column_A) OVER (ORDER BY row_id) AS Prev_Column_A,
        LAG(Column_A, 2) OVER (ORDER BY row_id) AS Prev_2_Column_A -- Two rows back
    FROM
        {{ ref('dummy') }}
)
SELECT
    Column_A,
    COALESCE(Column_B, Prev_Column_A) AS Column_B
    -- COALESCE(Column_B, Prev_2_Column_A) AS Column_B -- have it look back 2 rows
FROM
    OrderedData