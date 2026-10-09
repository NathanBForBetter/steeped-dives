WITH
    DistinctValues AS (
        SELECT
            attribute_cat,
            MAX(attribute_type) AS attribute_type,
            MAX(attribute_source) AS attribute_source
        FROM {{ ref('int_board_games_categories') }}
        WHERE attribute_cat IS NOT NULL
        GROUP BY 1
    )
SELECT 
    {{ dbt_utils.generate_surrogate_key(['attribute_cat']) }} AS attribute_key,
    *
FROM DistinctValues