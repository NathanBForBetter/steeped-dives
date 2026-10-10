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
    )
SELECT
    number_chosen,
    ai_share_pct,
    people_share_pct,
    computer_share_pct,
    -- differences in share points. Positive means the first source picked the
    -- number more often.
    ai_share_pct - computer_share_pct AS ai_vs_computer_diff,
    people_share_pct - computer_share_pct AS people_vs_computer_diff,
    ai_share_pct - people_share_pct AS ai_vs_people_diff
FROM ShareWide