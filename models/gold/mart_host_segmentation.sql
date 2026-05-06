SELECT
    city,
    host_type,
    COUNT(DISTINCT host_id)                     AS total_hosts,
    COUNT(listing_id)                           AS total_listings,
    ROUND(AVG(price_per_night), 2)              AS avg_price_per_night,
    ROUND(PERCENTILE(price_per_night, 0.5), 2)  AS median_price_per_night,
    ROUND(AVG(review_score), 2)                 AS avg_review_score,
    ROUND(AVG(availability_365), 0)             AS avg_availability_days
FROM {{ ref('int_listings_enriched') }}
GROUP BY city, host_type
ORDER BY city, host_type