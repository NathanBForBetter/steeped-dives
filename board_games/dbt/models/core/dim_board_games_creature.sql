WITH
    DistinctValues AS (
        SELECT
            creature_animal_cat,
            MAX(creature_animal_type) AS creature_animal_type,
            MAX(creature_animal_source) AS creature_animal_source
        FROM {{ ref('int_board_games_categories') }}
        WHERE creature_animal_cat IS NOT NULL
        GROUP BY 1
    )
SELECT 
    {{ dbt_utils.generate_surrogate_key(['creature_animal_cat']) }} AS creature_key,
    *
FROM DistinctValues