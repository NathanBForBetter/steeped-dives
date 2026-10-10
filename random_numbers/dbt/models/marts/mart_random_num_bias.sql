WITH
    RawData AS (
        SELECT
            N.number_chosen,
            F.source,
            F.choice_count_share AS choice_share
        FROM {{ ref('fct_random_num') }} AS F
        LEFT JOIN {{ ref('dim_random_num_chosen') }} AS N
            USING (number_key)
    ),
    ShareWide AS (
        SELECT
            number_chosen,
            COALESCE(SUM(choice_share)
                FILTER (WHERE source = 'ai'), 0) AS ai_share_pct,
            COALESCE(SUM(choice_share)
                FILTER (WHERE source = 'people'), 0) AS people_share_pct,
            COALESCE(SUM(choice_share)
                FILTER (WHERE source = 'computer'), 0) AS computer_share_pct
        FROM RawData
        GROUP BY number_chosen
    ),
    SourceShares AS (
        SELECT
            number_chosen,
            'ai' AS source,
            'AI (ChatGPT)' AS source_label,
            ai_share_pct AS source_share_pct,
            computer_share_pct
        FROM ShareWide
        UNION ALL
        SELECT
            number_chosen,
            'people' AS source,
            'People' AS source_label,
            people_share_pct AS source_share_pct,
            computer_share_pct
        FROM ShareWide
    ),
    Differences AS (
        SELECT
            *,
            ROUND(source_share_pct - computer_share_pct, 4)
                AS extra_share_points,
            CASE
                WHEN computer_share_pct > 0
                    THEN ROUND(source_share_pct / computer_share_pct, 2)
                ELSE NULL
            END AS times_vs_random
        FROM SourceShares
    ),
    Ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY source
                ORDER BY extra_share_points DESC, number_chosen ASC
            ) AS over_rank,
            ROW_NUMBER() OVER (
                PARTITION BY source
                ORDER BY extra_share_points ASC, number_chosen ASC
            ) AS under_rank
        FROM Differences
    )
SELECT
    source,
    source_label,
    number_chosen,
    ROUND(source_share_pct, 4) AS source_share_pct,
    ROUND(computer_share_pct, 4) AS computer_share_pct,
    extra_share_points,
    times_vs_random,
    'over-picked' AS direction
FROM Ranked
WHERE over_rank <= 15
UNION ALL
SELECT
    source,
    source_label,
    number_chosen,
    ROUND(source_share_pct, 4) AS source_share_pct,
    ROUND(computer_share_pct, 4) AS computer_share_pct,
    extra_share_points,
    times_vs_random,
    'under-picked' AS direction
FROM Ranked
WHERE under_rank <= 15
ORDER BY source, direction, extra_share_points DESC, number_chosen