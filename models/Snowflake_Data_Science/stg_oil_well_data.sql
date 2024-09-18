{{ config(materialized='table') }}

SELECT
    well_id,
    depth,
    porosity,
    permeability,
    thickness,
    is_productive,
    -- Add some engineered features
    depth * porosity AS depth_porosity_product,
    permeability * thickness AS perm_thickness_product
FROM {{ ref('oil_well_data') }}