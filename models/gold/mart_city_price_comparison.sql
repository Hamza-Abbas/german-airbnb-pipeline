SELECT
    city,
    room_type,
    COUNT(listing_id)                           AS total_listings,
    ROUND(AVG(price_per_night), 2)              AS avg_price_per_night,
    ROUND(PERCENTILE(price_per_night, 0.5), 2)  AS median_price_per_night,
    ROUND(MIN(price_per_night), 2)              AS min_price,
    ROUND(MAX(price_per_night), 2)              AS max_price
FROM {{ ref('int_listings_enriched') }}
GROUP BY city, room_type
ORDER BY city, avg_price_per_night DESC