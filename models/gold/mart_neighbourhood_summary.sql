SELECT
    city,
    neighbourhood,
    COUNT(listing_id)                           AS total_listings,
    ROUND(AVG(price_per_night), 2)              AS avg_price_per_night,
    ROUND(PERCENTILE(price_per_night, 0.5), 2)  AS median_price,
    SUM(number_of_reviews)                      AS total_reviews,
    ROUND(AVG(review_score), 2)                 AS avg_review_score,
    ROUND(AVG(availability_365), 0)             AS avg_availability_days,
    ROUND(
        SUM(number_of_reviews) * 1.0 / COUNT(listing_id), 2
    )                                           AS reviews_per_listing
FROM {{ ref('int_listings_enriched') }}
GROUP BY city, neighbourhood
HAVING COUNT(listing_id) >= 20
ORDER BY city, total_listings DESC