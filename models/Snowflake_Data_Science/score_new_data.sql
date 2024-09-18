{{ config(
    materialized='table',
    tags=['score']
) }}

WITH latest_model AS MODEL OIL_WELL_PRODUCTIVITY_MODEL VERSION LAST

SELECT 
    n.*,
    -- Extract the 'output_feature_0' value from the JSON object
    (latest_model!predict(
        n.DEPTH, 
        n.POROSITY, 
        n.PERMEABILITY, 
        n.THICKNESS, 
        n.DEPTH_POROSITY_PRODUCT, 
        n.PERM_THICKNESS_PRODUCT
    )::variant):output_feature_0::int AS PREDICTED_PRODUCTIVITY
FROM {{ ref('new_oil_well_data') }} AS n
