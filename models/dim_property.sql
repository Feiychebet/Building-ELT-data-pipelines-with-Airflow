-- models/dim_property.sql
{{ config(materialized='table') }}

with property_data as (
    select
        listing_id as property_id,
        listing_neighbourhood as neighbourhood,
        property_type,
        room_type,
        accommodates,
        amenities,
        price,
        has_availability,
        host_id
    from {{ ref('listings_all_stg') }}
),

distinct_properties as (
    select
        property_id,
        neighbourhood,
        property_type,
        room_type,
        accommodates,
        amenities,
        price,
        max(has_availability) as has_availability, --  to know if it ever has availability
        host_id,
        row_number() over (
            partition by property_id
            order by has_availability desc, price asc -
        ) as rn
    from property_data
    group by property_id, neighbourhood, property_type, room_type, accommodates, amenities, price, host_id
)

select
    property_id,
    neighbourhood,
    property_type,
    room_type,
    accommodates,
    amenities,
    price,
    has_availability,
    host_id
from distinct_properties
where rn = 1 -- gets the most recent/available/cheapest listing per property
