WITH
    DistinctValues AS (
        SELECT
            fantasy_adventure_cat,
            MAX(fantasy_adventure_type) AS fantasy_adventure_type,
            MAX(fantasy_adventure_source) AS fantasy_adventure_source
        FROM {{ ref('int_board_games_categories') }}
        WHERE fantasy_adventure_cat IS NOT NULL
        GROUP BY 1
    )
SELECT 
    {{ dbt_utils.generate_surrogate_key(['fantasy_adventure_cat']) }} AS fantasy_key,
    *
FROM DistinctValues