-- models/nsw_lga_suburb.sql
SELECT * FROM {{ source('raw', 'nsw_lga_suburb') }}
