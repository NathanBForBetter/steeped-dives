WITH
    DistinctValues AS (
        SELECT
            industry_occupation_cat,
            MAX(industry_occupation_type) AS industry_occupation_type,
            MAX(industry_occupation_source) AS industry_occupation_source
        FROM {{ ref('int_board_games_categories') }}
        WHERE industry_occupation_cat IS NOT NULL
        GROUP BY 1
    )
SELECT 
    {{ dbt_utils.generate_surrogate_key(['industry_occupation_cat']) }} AS industry_key,
    *
FROM DistinctValues