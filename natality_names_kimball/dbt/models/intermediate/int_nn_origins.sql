-- One row per name: origins and spelling variants, merged from the two
-- sources (BehindTheName and the Ancestry first-name pages) and ranked so the
-- single most distinctive origin can be read off without a join.
--
-- Three Spark behaviors shape the SQL below:
--
--   * array_union() returns NULL when either side is NULL, and size(NULL) is
--     -1. A name found in only one source has a NULL array on the other
--     side, so both sides are coalesced to an empty array before the union.
--   * ARRAY(NULL) is a one-element array, which size() counts as 1. A missing
--     variations field becomes an empty array.
--   * array_agg_order_by() collects structs so sort_array can order by the
--     count. Element [0] is a struct, so the final select reads [0].value to
--     get the string.
WITH
    BTNOriginsMultiRows AS (
        SELECT
            name,
            COUNT(*) AS row_count
        FROM {{ ref('stg_nn_origins_btn') }}
        GROUP BY 1
    ),
    BTNMultiOriginsVariants AS (
        SELECT
            name,
            origin,
            origin_2,
            variant_of,
            female,
            male,
            -- These either are true or null with END.
            -- Helpful with Coalesce with CTE below.
            CASE 
                WHEN origin_2 IS NOT NULL
                    AND variant_of IS NOT NULL
                    AND female IS NOT NULL 
                    AND male IS NOT NULL
                    THEN TRUE
                END AS is_pick_1,
            CASE
                WHEN origin_2 IS NOT NULL
                    AND variant_of IS NOT NULL
                    AND (female IS NOT NULL OR male IS NOT NULL)
                    THEN TRUE 
                END AS is_pick_2,
            CASE
                WHEN variant_of IS NOT NULL
                    AND (female IS NOT NULL OR male IS NOT NULL)
                    THEN TRUE 
                END
                AS is_pick_3,
            CASE 
                WHEN origin_2 IS NOT NULL
                    AND (female IS NOT NULL OR male IS NOT NULL) 
                    THEN TRUE
                END AS is_pick_4,
            -- Idc if this knocks out row with false.
            -- But test for this table before makes unlikely.
            (female IS NOT NULL OR male IS NOT NULL)
                AS is_pick_5             
        FROM {{ ref('stg_nn_origins_btn') }}
        WHERE name IN (
            SELECT name FROM BTNOriginsMultiRows
            WHERE row_count > 1
        ) 
    ),
    BTNMultipleDedupe AS (
        SELECT
            name,
            CASE
                WHEN ANY_VALUE(origin_2) IS NULL THEN ARRAY(ANY_VALUE(origin))
                ELSE ARRAY(ANY_VALUE(origin), ANY_VALUE(origin_2))
                END AS origin_list,
            ARRAY_AGG(DISTINCT variant_of) AS variant_list,
            ANY_VALUE(male) AS male,
            ANY_VALUE(female) AS female
        FROM BTNMultiOriginsVariants
        -- Gotta be TRUE
        WHERE COALESCE(is_pick_1, is_pick_2, is_pick_3, is_pick_4, is_pick_5)
        GROUP BY 1
    ),
    BTNSingleDedupe AS (
        SELECT
            name,
            CASE
                WHEN ANY_VALUE(origin_2) IS NULL THEN ARRAY(ANY_VALUE(origin))
                ELSE ARRAY(ANY_VALUE(origin), ANY_VALUE(origin_2))
                END AS origin_list,
            ARRAY_AGG(DISTINCT variant_of) AS variant_list,
            ANY_VALUE(male) AS male,
            ANY_VALUE(female) AS female
        FROM {{ ref('stg_nn_origins_btn') }}
        WHERE name IN (
            SELECT name FROM BTNOriginsMultiRows
            WHERE row_count = 1
        ) 
        GROUP BY 1
    ),
    BTNUnion AS (
        SELECT * FROM BTNSingleDedupe
        UNION ALL
        SELECT * FROM BTNMultipleDedupe
    ),
    AncestryFormat AS (
        SELECT
            name,
            CASE
                WHEN origin IS NULL OR TRIM(origin) = '' THEN ARRAY()
                WHEN origin LIKE '%,%' THEN SPLIT(origin, ', ')
                ELSE ARRAY(origin)
                END AS origin_list,
            -- ARRAY(NULL) is a one-element array holding NULL, which size()
            -- counts as 1. An absent variations field has to become an empty
            -- array, not an array of one nothing.
            CASE
                WHEN variations IS NULL OR TRIM(variations) = '' THEN ARRAY()
                WHEN variations LIKE '%,%' THEN SPLIT(variations, ', ')
                ELSE ARRAY(variations)
                END AS variant_list
        FROM {{ ref('stg_nn_origins_ancestry') }}
    ),
    CombineOrigins AS (
        SELECT
            COALESCE(B.name, A.name) AS name,
            -- array_union(NULL, x) is NULL in Spark, and a FULL OUTER JOIN
            -- guarantees one side is NULL for every name that is in only one
            -- source. Coalesce both sides or those names report size() = -1.
            ARRAY_UNION(
                COALESCE(B.origin_list, ARRAY()),
                COALESCE(A.origin_list, ARRAY())) AS origin_list,
            ARRAY_UNION(
                COALESCE(B.variant_list, ARRAY()),
                COALESCE(A.variant_list, ARRAY())) AS variation_list,
            B.male,
            B.female
        FROM BTNUnion AS B
        FULL OUTER JOIN AncestryFormat AS A USING(name)
    ),
    OriginsExplode AS (
        SELECT
            name,
            origin
        FROM CombineOrigins
        LATERAL VIEW EXPLODE(origin_list) AS origin
    ),
    OriginNameCount AS (
        SELECT 
            origin,
            COUNT(name) AS name_count
        FROM OriginsExplode
        GROUP BY 1
    ),
    OriginList AS (
        SELECT
            E.name,
            -- ascending=False puts the highest name_count at [0], so the
            -- most widely-shared origin is the one called "main"
            {{ array_agg_order_by('E.origin', 'C.name_count', FALSE) }} AS origins_ordered
        FROM OriginsExplode AS E
        LEFT JOIN OriginNameCount AS C USING(origin)
        GROUP BY 1
    ),
    VariationsExplode AS (
        SELECT
            name,
            variation
        FROM CombineOrigins
        LATERAL VIEW EXPLODE(variation_list) AS variation
    ),
    VariationNameCount AS (
        SELECT 
            variation,
            COUNT(name) AS name_count
        FROM VariationsExplode
        GROUP BY 1
    ),
    VariationList AS (
        SELECT
            E.name,
            {{ array_agg_order_by('E.variation', 'C.name_count', FALSE) }} AS variation_ordered
        FROM VariationsExplode AS E
        LEFT JOIN VariationNameCount AS C USING(variation)
        GROUP BY 1
    )
SELECT
    C.name,
    CASE
        WHEN C.male IS NOT NULL AND C.female IS NOT NULL THEN 'either'
        WHEN C.male IS NOT NULL THEN 'male'
        WHEN C.female IS NOT NULL THEN 'female'
        END AS name_gender,
    SIZE(C.origin_list) AS origin_count,
    -- [0] is the STRUCT sort_array ordered; .origin is the value inside it
    O.origins_ordered[0].value AS main_origin,
    SIZE(C.variation_list) AS variation_count,
    V.variation_ordered[0].value AS main_variation
FROM CombineOrigins AS C
LEFT JOIN OriginList AS O USING(name)
LEFT JOIN VariationList AS V USING(name)