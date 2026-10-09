-- Politics and government categories, one row per bucket. One of the 13
-- category dimensions built from int_board_games_categories.
WITH
    DistinctValues AS (
        SELECT
            politics_gov_cat,
            MAX(politics_gov_type) AS politics_gov_type,
            MAX(politics_gov_source) AS politics_gov_source
        FROM {{ ref('int_board_games_categories') }}
        WHERE politics_gov_cat IS NOT NULL
        GROUP BY 1
    )
SELECT
    {{ dbt_utils.generate_surrogate_key(['politics_gov_cat']) }} AS politics_key,
    *
FROM DistinctValues
