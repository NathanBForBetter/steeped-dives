WITH
    DistinctValues AS (
        SELECT
            component_cat,
            MAX(component_type) AS component_type,
            MAX(component_source) AS component_source
        FROM {{ ref('int_board_games_categories') }}
        WHERE component_cat IS NOT NULL
        GROUP BY 1
    )
SELECT 
    {{ dbt_utils.generate_surrogate_key(['component_cat']) }} AS component_key,
    *
FROM DistinctValues