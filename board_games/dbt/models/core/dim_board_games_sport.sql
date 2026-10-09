WITH
    DistinctValues AS (
        SELECT
            sport_cat,
            MAX(sport_type) AS sport_type,
            MAX(sport_source) AS sport_source
        FROM {{ ref('int_board_games_categories') }}
        WHERE sport_cat IS NOT NULL
        GROUP BY 1
    )
SELECT 
    {{ dbt_utils.generate_surrogate_key(['sport_cat']) }} AS sport_key,
    *
FROM DistinctValues