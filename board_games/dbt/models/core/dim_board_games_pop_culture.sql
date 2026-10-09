WITH
    DistinctValues AS (
        SELECT
            pop_culture_cat,
            MAX(pop_culture_type) AS pop_culture_type,
            MAX(pop_culture_source) AS pop_culture_source
        FROM {{ ref('int_board_games_categories') }}
        WHERE pop_culture_cat IS NOT NULL
        GROUP BY 1
    )
SELECT 
    {{ dbt_utils.generate_surrogate_key(['pop_culture_cat']) }} AS pop_culture_key,
    *
FROM DistinctValues