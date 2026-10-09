WITH
    DistinctValues AS (
        SELECT
            history_war_cat,
            MAX(history_war_type) AS history_war_type,
            MAX(history_war_source) AS history_war_source
        FROM {{ ref('int_board_games_categories') }}
        WHERE history_war_cat IS NOT NULL
        GROUP BY 1
    )
SELECT 
    {{ dbt_utils.generate_surrogate_key(['history_war_cat']) }} AS history_key,
    *
FROM DistinctValues