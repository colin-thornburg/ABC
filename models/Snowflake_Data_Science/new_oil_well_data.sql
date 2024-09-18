{{ config(materialized='table') }}

WITH new_wells AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY RANDOM()) + 1000 AS well_id,
        3000 + UNIFORM(0, 4000, RANDOM()) AS depth,
        0.05 + UNIFORM(0, 0.2, RANDOM()) AS porosity,
        10 + UNIFORM(0, 90, RANDOM()) AS permeability,
        50 + UNIFORM(0, 100, RANDOM()) AS thickness
    FROM TABLE(GENERATOR(ROWCOUNT => 10))
)
SELECT
    well_id,
    depth AS DEPTH,
    porosity AS POROSITY,
    permeability AS PERMEABILITY,
    thickness AS THICKNESS,
    depth * porosity AS DEPTH_POROSITY_PRODUCT,
    permeability * thickness AS PERM_THICKNESS_PRODUCT
FROM new_wells