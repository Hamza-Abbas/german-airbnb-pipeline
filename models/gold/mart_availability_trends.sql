SELECT
    city,
    DATE_FORMAT(CAST(DATE_TRUNC('month', calendar_date) AS DATE), 'yyyy-MM') AS month,
    COUNT(*)                                    AS total_listing_days,
    SUM(CASE WHEN is_available THEN 1 ELSE 0 END)   AS available_listing_days,
    SUM(CASE WHEN NOT is_available THEN 1 ELSE 0 END) AS booked_listing_days,
    ROUND(
        SUM(CASE WHEN NOT is_available THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
    )                                           AS occupancy_rate_pct
FROM {{ ref('stg_calendar') }}
WHERE is_available IS NOT NULL
GROUP BY city, DATE_FORMAT(CAST(DATE_TRUNC('month', calendar_date) AS DATE), 'yyyy-MM')
ORDER BY city, month DESC