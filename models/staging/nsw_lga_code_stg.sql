{{ config(materialized='view') }}

WITH standardized_data AS (
  SELECT
    CAST(LGA_CODE as INT) as lga_code,
    TRIM(UPPER(LGA_NAME)) as lga_name -- Standardize the LGA names to uppercase and remove any leading/trailing spaces
  FROM {{ source('raw', 'nsw_lga_code') }}
)

SELECT *
FROM standardized_data;
