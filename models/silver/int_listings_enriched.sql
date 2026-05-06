SELECT
    listing_id,
    listing_name,
    host_id,
    host_name,
    host_since,
    host_listings_count,
    neighbourhood,
    latitude,
    longitude,
    room_type,
    accommodates,
    bedrooms,
    beds,
    price_per_night,
    number_of_reviews,
    review_score,
    availability_365,
    minimum_nights,
    city,

    -- Host type segmentation
    CASE
        WHEN host_listings_count >= 10 THEN 'Professional'
        WHEN host_listings_count BETWEEN 2 AND 9 THEN 'Semi-Professional'
        ELSE 'Casual'
    END AS host_type,

    -- Price segmentation
    CASE
        WHEN price_per_night < 50  THEN 'Budget'
        WHEN price_per_night < 100 THEN 'Mid-Range'
        WHEN price_per_night < 200 THEN 'Premium'
        ELSE 'Luxury'
    END AS price_segment,

    -- Availability category
    CASE
        WHEN availability_365 < 60  THEN 'Rarely Available'
        WHEN availability_365 < 180 THEN 'Occasionally Available'
        ELSE 'Highly Available'
    END AS availability_category,

    -- Years hosting
    DATEDIFF(CURRENT_DATE(), host_since) / 365.0 AS years_hosting

FROM {{ ref('stg_listings') }}
WHERE price_per_night IS NOT NULL
  AND price_per_night BETWEEN 10 AND 10000