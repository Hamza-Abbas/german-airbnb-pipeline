WITH berlin AS (
    SELECT
        CAST(id AS BIGINT)                                          AS listing_id,
        name                                                        AS listing_name,
        CAST(host_id AS BIGINT)                                     AS host_id,
        host_name,
        CAST(host_since AS DATE)                                    AS host_since,
        CAST(host_listings_count AS INT)                            AS host_listings_count,
        neighbourhood_cleansed                                      AS neighbourhood,
        CAST(latitude AS DOUBLE)                                    AS latitude,
        CAST(longitude AS DOUBLE)                                   AS longitude,
        room_type,
        CAST(accommodates AS INT)                                   AS accommodates,
        CAST(bedrooms AS INT)                                       AS bedrooms,
        CAST(beds AS INT)                                           AS beds,
        TRY_CAST(
            REGEXP_REPLACE(price, '[$,]', '') AS DECIMAL(10,2)
        )                                                           AS price_per_night,
        CAST(number_of_reviews AS INT)                              AS number_of_reviews,
        review_scores_rating                                        AS review_score,
        CAST(availability_365 AS INT)                               AS availability_365,
        CAST(minimum_nights AS INT)                                 AS minimum_nights,
        'Berlin'                                                    AS city
    FROM {{ ref('berlin_listings') }}
    WHERE REGEXP_REPLACE(price, '[$,]', '') REGEXP '^[0-9]+(\.[0-9]+)?$'
      AND TRY_CAST(REGEXP_REPLACE(price, '[$,]', '') AS DECIMAL(10,2)) > 0
),

munich AS (
    SELECT
        CAST(id AS BIGINT)                                          AS listing_id,
        name                                                        AS listing_name,
        CAST(host_id AS BIGINT)                                     AS host_id,
        host_name,
        CAST(host_since AS DATE)                                    AS host_since,
        CAST(host_listings_count AS INT)                            AS host_listings_count,
        neighbourhood_cleansed                                      AS neighbourhood,
        CAST(latitude AS DOUBLE)                                    AS latitude,
        CAST(longitude AS DOUBLE)                                   AS longitude,
        room_type,
        CAST(accommodates AS INT)                                   AS accommodates,
        CAST(bedrooms AS INT)                                       AS bedrooms,
        CAST(beds AS INT)                                           AS beds,
        TRY_CAST(
            REGEXP_REPLACE(price, '[$,]', '') AS DECIMAL(10,2)
        )                                                           AS price_per_night,
        CAST(number_of_reviews AS INT)                              AS number_of_reviews,
        review_scores_rating                                        AS review_score,
        CAST(availability_365 AS INT)                               AS availability_365,
        CAST(minimum_nights AS INT)                                 AS minimum_nights,
        'Munich'                                                    AS city
    FROM {{ ref('munich_listings') }}
    WHERE REGEXP_REPLACE(price, '[$,]', '') REGEXP '^[0-9]+(\.[0-9]+)?$'
      AND TRY_CAST(REGEXP_REPLACE(price, '[$,]', '') AS DECIMAL(10,2)) > 0
)

SELECT * FROM berlin
UNION ALL
SELECT * FROM munich