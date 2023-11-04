{{ config(materialized='view') }}

WITH transformed_data AS (
  SELECT
    LGA_CODE_2016,
    CAST(Median_age_persons AS INT) AS median_age_persons,
    CAST(Median_mortgage_repay_monthly AS INT) AS median_mortgage_repayment_monthly,
    CAST(Median_tot_prsnl_inc_weekly AS INT) AS median_total_personal_income_weekly,
    CAST(Median_rent_weekly AS INT) AS median_rent_weekly,
    CAST(Median_tot_fam_inc_weekly AS INT) AS median_total_family_income_weekly,
    CAST(Average_num_psns_per_bedroom AS FLOAT) AS average_number_persons_per_bedroom,
    CAST(Median_tot_hhd_inc_weekly AS INT) AS median_total_household_income_weekly,
    CAST(Average_household_size AS FLOAT) AS average_household_size
  FROM {{ source('raw', 'census_g02_nsw_lga') }}
)

SELECT *
FROM transformed_data;
