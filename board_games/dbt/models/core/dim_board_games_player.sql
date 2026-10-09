WITH
    DistinctValues AS (
        SELECT
            player_audience_cat,
            MAX(player_audience_type) AS player_audience_type,
            MAX(player_audience_source) AS player_audience_source
        FROM {{ ref('int_board_games_categories') }}
        WHERE player_audience_cat IS NOT NULL
        GROUP BY 1
    )
SELECT 
    {{ dbt_utils.generate_surrogate_key(['player_audience_cat']) }} AS player_key,
    *
FROM DistinctValues