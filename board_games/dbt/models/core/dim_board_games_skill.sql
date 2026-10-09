WITH
    DistinctValues AS (
        SELECT
            skill_cat,
            MAX(skill_type) AS skill_type,
            MAX(skill_source) AS skill_source
        FROM {{ ref('int_board_games_categories') }}
        WHERE skill_cat IS NOT NULL
        GROUP BY 1
    )
SELECT 
    {{ dbt_utils.generate_surrogate_key(['skill_cat']) }} AS skill_key,
    *
FROM DistinctValues