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

    SourcePairs AS (
        SELECT
            R1.number_chosen,
            R1.source AS source_1,
            R2.source AS source_2,
            R1.choice_share AS source_1_share,
            R2.choice_share AS source_2_share
        FROM RawData AS R1
        INNER JOIN RawData AS R2
            ON R1.number_chosen = R2.number_chosen
            AND R1.source < R2.source
    ),

    MaxDiffNumber AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY source_1, source_2
                ORDER BY ABS(source_1_share - source_2_share) DESC,
                         number_chosen ASC
            ) AS diff_rank
        FROM SourcePairs
    ),

    DivergenceMetrics AS (
        SELECT
            source_1,
            source_2,
            {{ divergence_aggs('source_1_share', 'source_2_share') }}
        FROM SourcePairs
        GROUP BY source_1, source_2
    ),

    -- People's distance from true random. The ratio columns measure every
    -- other distance from true random against it.
    PeopleVsRandom AS (
        SELECT
            divergence_perc,
            js_divergence
        FROM DivergenceMetrics
        WHERE 'people' IN (source_1, source_2)
            AND 'computer' IN (source_1, source_2)
    )
SELECT
    D.*,
    M.number_chosen AS number_at_max_diff,
    -- Filled on the pairs that include true random. On the ChatGPT row this
    -- is how many times further from random ChatGPT is than people are. On
    -- the people row it is 1.
    CASE
        WHEN 'computer' IN (D.source_1, D.source_2)
            THEN D.divergence_perc / P.divergence_perc
        END AS times_further_from_random_than_people_tvd,
    CASE
        WHEN 'computer' IN (D.source_1, D.source_2)
            THEN D.js_divergence / P.js_divergence
        END AS times_further_from_random_than_people_jsd
FROM DivergenceMetrics AS D
INNER JOIN MaxDiffNumber AS M
    ON D.source_1 = M.source_1
    AND D.source_2 = M.source_2
    AND M.diff_rank = 1
LEFT JOIN PeopleVsRandom AS P ON TRUE
ORDER BY D.source_1, D.source_2