WITH
    DistinctValues AS (
        SELECT
            transportation_travel_cat,
            MAX(transportation_travel_type) AS transportation_travel_type,
            MAX(transportation_travel_source) AS transportation_travel_source
        FROM {{ ref('int_board_games_categories') }}
        WHERE transportation_travel_cat IS NOT NULL
        GROUP BY 1
    )
SELECT 
    {{ dbt_utils.generate_surrogate_key(['transportation_travel_cat']) }} AS transportation_key,
    *
FROM DistinctValues