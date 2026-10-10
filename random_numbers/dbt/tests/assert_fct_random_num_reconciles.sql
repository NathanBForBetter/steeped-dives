-- Each source has a row for all 100 numbers, every pick in staging is counted
-- once, and the shares add up to 100. Returns the sources where one of those
-- does not hold.
WITH
    Staged AS (
        SELECT 'ai' AS source, COUNT(*) AS pick_count
        FROM {{ ref('stg_random_num_ai') }}
        UNION ALL
        SELECT 'people' AS source, COUNT(*) AS pick_count
        FROM {{ ref('stg_random_num_people') }}
        UNION ALL
        SELECT 'computer' AS source, COUNT(*) AS pick_count
        FROM {{ ref('stg_random_num_true') }}
    ),
    Fact AS (
        SELECT
            source,
            COUNT(*) AS number_count,
            SUM(choice_count) AS pick_count,
            SUM(choice_count_share) AS share_sum
        FROM {{ ref('fct_random_num') }}
        GROUP BY 1
    )
SELECT
    S.source,
    S.pick_count AS staged_pick_count,
    F.pick_count AS fact_pick_count,
    F.number_count,
    F.share_sum
FROM Staged AS S
LEFT JOIN Fact AS F USING (source)
WHERE F.source IS NULL
    OR F.number_count != 100
    OR F.pick_count != S.pick_count
    OR ABS(F.share_sum - 100) > 0.000001
