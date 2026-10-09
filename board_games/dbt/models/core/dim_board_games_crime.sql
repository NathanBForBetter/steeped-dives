WITH
    DistinctValues AS (
        SELECT
            crime_violence_cat,
            MAX(crime_violence_type) AS crime_violence_type,
            MAX(crime_violence_source) AS crime_violence_source
        FROM {{ ref('int_board_games_categories') }}
        WHERE crime_violence_cat IS NOT NULL
        GROUP BY 1
    )
SELECT 
    {{ dbt_utils.generate_surrogate_key(['crime_violence_cat']) }} AS crime_key,
    *
FROM DistinctValues