SELECT COUNT(*) FROM {{ ref('berlin_reviews') }} --667476

SELECT COUNT(*) FROM {{ ref('munich_reviews') }} --236690

SELECT 236690 + 667476