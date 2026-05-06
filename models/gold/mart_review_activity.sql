SELECT
    city,
    DATE_FORMAT(CAST(DATE_TRUNC('month', review_date) AS DATE), 'yyyy-MM')           AS review_month,
    COUNT(review_id)                            AS total_reviews,
    COUNT(DISTINCT listing_id)                  AS unique_listings_reviewed,
    COUNT(DISTINCT reviewer_id)                 AS unique_reviewers
FROM {{ ref('stg_reviews') }}
WHERE review_date >= '2015-01-01'
  AND review_date <= CURRENT_DATE()
GROUP BY city, DATE_FORMAT(CAST(DATE_TRUNC('month', review_date) AS DATE), 'yyyy-MM') 
ORDER BY city, review_month
