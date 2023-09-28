{{ config(
    materialized='incremental', 
    unique_key='business_key',
    post_hook='DELETE FROM {{ this }} WHERE DATEADD(day, -60, CURRENT_TIMESTAMP()) > Load_Timestamp;'
) }}


WITH combined_sources AS (
  SELECT 
      business_key,
      attribute,
      source,
      Load_Timestamp
  FROM {{ ref('source1') }}
  UNION ALL
  SELECT 
      business_key,
      attribute,
      source,
      Load_Timestamp
  FROM {{ ref('source2') }}
  UNION ALL
  SELECT 
      business_key,
      attribute,
      source,
      Load_Timestamp
  FROM {{ ref('source3') }}
),
deduplicated as (
  SELECT
    business_key,
    MAX(attribute) as attribute,
    MAX(source) as source,
    MAX(Load_Timestamp) as Load_Timestamp
  FROM combined_sources
  GROUP BY business_key
)

SELECT * FROM deduplicated

{% if is_incremental() %}
  WHERE Load_Timestamp > (SELECT MAX(Load_Timestamp) FROM {{ this }})
{% endif %}
