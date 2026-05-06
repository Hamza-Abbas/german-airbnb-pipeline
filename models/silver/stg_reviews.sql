WITH berlin AS (
    SELECT
        CAST(id AS BIGINT)          AS review_id,
        CAST(listing_id AS BIGINT)  AS listing_id,
        CAST(reviewer_id AS BIGINT) AS reviewer_id,
        reviewer_name,
        CAST(date AS DATE)          AS review_date,
        comments,
        'Berlin'                    AS city
    FROM {{ ref('berlin_reviews') }}
    WHERE date IS NOT NULL
      AND listing_id IS NOT NULL
),

munich AS (
    SELECT
        CAST(id AS BIGINT)          AS review_id,
        CAST(listing_id AS BIGINT)  AS listing_id,
        CAST(reviewer_id AS BIGINT) AS reviewer_id,
        reviewer_name,
        CAST(date AS DATE)          AS review_date,
        comments,
        'Munich'                    AS city
    FROM {{ ref('munich_reviews') }}
    WHERE date IS NOT NULL
      AND listing_id IS NOT NULL
)

SELECT * FROM berlin
UNION ALL
SELECT * FROM munich