WITH berlin AS (
    SELECT
        CAST(listing_id AS BIGINT)  AS listing_id,
        CAST(date AS DATE)          AS calendar_date,
        CASE 
            WHEN available = 't' THEN TRUE
            WHEN available = 'f' THEN FALSE
            ELSE NULL
        END                         AS is_available,
        CAST(minimum_nights AS INT) AS minimum_nights,
        'Berlin'                    AS city
    FROM {{ ref('berlin_calendar') }}
    WHERE date IS NOT NULL
      AND listing_id IS NOT NULL
),

munich AS (
    SELECT
        CAST(listing_id AS BIGINT)  AS listing_id,
        CAST(date AS DATE)          AS calendar_date,
        CASE 
            WHEN available = 't' THEN TRUE
            WHEN available = 'f' THEN FALSE
            ELSE NULL
        END                         AS is_available,
        CAST(minimum_nights AS INT) AS minimum_nights,
        'Munich'                    AS city
    FROM {{ ref('munich_calendar') }}
    WHERE date IS NOT NULL
      AND listing_id IS NOT NULL
)

SELECT * FROM berlin
UNION ALL
SELECT * FROM munich