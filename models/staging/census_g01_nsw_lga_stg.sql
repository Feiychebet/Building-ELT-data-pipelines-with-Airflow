{{ config(materialized='view') }}

WITH transformed_data AS (
  SELECT
    LGA_CODE_2016,
    CAST(Tot_P_M AS INT) AS total_persons_male,
    CAST(Tot_P_F AS INT) AS total_persons_female,
    CAST(Tot_P_P AS INT) AS total_persons,
    CAST(Age_0_4_yr_M AS INT) AS age_0_4_years_male,
    CAST(Age_0_4_yr_F AS INT) AS age_0_4_years_female,
    CAST(Age_0_4_yr_P AS INT) AS age_0_4_years_total,
  FROM {{ source('raw', 'census_g01_nsw_lga') }}
)

SELECT *
FROM transformed_data;
