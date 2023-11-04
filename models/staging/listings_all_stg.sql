#for listings
{{ config(materialized='view') }}

WITH cleaned_data AS (
  SELECT
    CAST(LISTING_ID as INT) as listing_id,
    CAST(SCRAPE_ID as BIGINT) as scrape_id,
    TO_DATE(SCRAPED_DATE, 'MM/DD/YYYY') as scraped_date,
    HOST_ID,
    HOST_NAME,
    TO_DATE(HOST_SINCE, 'DD/MM/YYYY') as host_since,
    CASE WHEN HOST_IS_SUPERHOST = 't' THEN TRUE ELSE FALSE END as host_is_superhost,
    HOST_NEIGHBOURHOOD,
    LISTING_NEIGHBOURHOOD,
    PROPERTY_TYPE,
    ROOM_TYPE,
    CAST(ACCOMMODATES as INT) as accommodates,
    CAST(REPLACE(PRICE, '$', '') as NUMERIC) as price,
    CASE WHEN HAS_AVAILABILITY = 't' THEN TRUE ELSE FALSE END as has_availability,
    CAST(AVAILABILITY_30 as INT) as availability_30,
    CAST(NUMBER_OF_REVIEWS as INT) as number_of_reviews,
    CAST(REVIEW_SCORES_RATING as INT) as review_scores_rating,
    -- ... continue for other review scores ...
    CAST(REVIEW_SCORES_ACCURACY as INT) as review_scores_accuracy,
    -- ... and so on for the rest of the review scores columns ...
    CAST(REVIEW_SCORES_VALUE as INT) as review_scores_value
  FROM {{ source('raw', 'listings_all') }}
  -- Include any necessary WHERE conditions to clean the data
)
SELECT *
FROM cleaned_data;
