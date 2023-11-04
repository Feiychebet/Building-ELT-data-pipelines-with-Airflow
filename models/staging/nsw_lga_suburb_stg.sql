{{ config(materialized='view') }}

WITH clean_data AS (
  SELECT
    TRIM(UPPER(LGA_NAME)) as lga_name,
    TRIM(UPPER(SUBURB_NAME)) as suburb_name
  FROM {{ source('raw', 'nsw_lga_suburb') }}
)

SELECT *
FROM clean_data;
