WITH
    RawData AS (
        SELECT
            N.number_chosen,
            N.number_tens_range,
            N.number_is_even,
            N.number_is_odd,
            N.number_last_digit,
            F.source,
            F.choice_count_share AS choice_share
        FROM {{ ref('fct_random_num') }} AS F
        LEFT JOIN {{ ref('dim_random_num_chosen') }} AS N
            USING (number_key)
    ),
    UnionPatterns AS (
        SELECT
            number_tens_range AS pattern,
            '10s range' AS pattern_type,
            COALESCE(SUM(choice_share)
                FILTER (WHERE source = 'ai'), 0) AS ai_share_pct,
            COALESCE(SUM(choice_share)
                FILTER (WHERE source = 'people'), 0) AS people_share_pct,
            COALESCE(SUM(choice_share)
                FILTER (WHERE source = 'computer'), 0) AS computer_share_pct
        FROM RawData
        GROUP BY 1
        UNION ALL
        SELECT
            CAST(number_is_even AS VARCHAR) AS pattern,
            'is even' AS pattern_type,
            COALESCE(SUM(choice_share)
                FILTER (WHERE source = 'ai'), 0) AS ai_share_pct,
            COALESCE(SUM(choice_share)
                FILTER (WHERE source = 'people'), 0) AS people_share_pct,
            COALESCE(SUM(choice_share)
                FILTER (WHERE source = 'computer'), 0) AS computer_share_pct
        FROM RawData
        GROUP BY 1
        UNION ALL
        SELECT
            CAST(number_last_digit AS VARCHAR) AS pattern,
            'last digit' AS pattern_type,
            COALESCE(SUM(choice_share)
                FILTER (WHERE source = 'ai'), 0) AS ai_share_pct,
            COALESCE(SUM(choice_share)
                FILTER (WHERE source = 'people'), 0) AS people_share_pct,
            COALESCE(SUM(choice_share)
                FILTER (WHERE source = 'computer'), 0) AS computer_share_pct
        FROM RawData
        GROUP BY 1
    )
SELECT
    pattern,
    pattern_type,
    ai_share_pct,
    people_share_pct,
    computer_share_pct,
    -- differences in share points. Positive means the first source picked the
    -- pattern more often.
    ai_share_pct - computer_share_pct AS ai_vs_computer_diff,
    people_share_pct - computer_share_pct AS people_vs_computer_diff,
    ai_share_pct - people_share_pct AS ai_vs_people_diff
FROM UnionPatterns
ORDER BY pattern_type, pattern