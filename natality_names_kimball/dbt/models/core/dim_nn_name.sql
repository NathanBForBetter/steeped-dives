-- Name dimension: one row per distinct first name, with the origin
-- attributes from the two scraped sources and three all-time usage measures.
--
-- The usage measures (total_name_count, total_name_count_1995_2025,
-- name_in_states_count) are aggregates stored on a dimension, which is a
-- deliberate choice rather than an oversight: Steeped AI profiles the name as
-- an entity and needs its all-time magnitude available without a rollup over
-- the 6.7M-row fact. They are additive over the dimension, never over the
-- fact - see the README section "Aggregates on a dimension".
WITH
    -- A unisex name such as Jordan or Avery is in the SSA file under both
    -- 'm' and 'f'. Its SSA gender goes by weight: whichever sex the name was
    -- given to more often wins, and a near-even split is labeled 'either'.
    SsaGenderWeight AS (
        SELECT
            name,
            SUM(CASE WHEN gender = 'female' THEN name_count ELSE 0 END) AS female_count,
            SUM(CASE WHEN gender = 'male' THEN name_count ELSE 0 END) AS male_count
        FROM {{ ref('int_nn_baby_names') }}
        GROUP BY 1
    ),
    SsaGender AS (
        SELECT
            name,
            female_count,
            male_count,
            CASE
                -- within 10 points of even counts as genuinely unisex
                WHEN female_count + male_count = 0 THEN NULL
                WHEN ABS(female_count - male_count)
                     / CAST(female_count + male_count AS DOUBLE) < 0.10 THEN 'either'
                WHEN female_count > male_count THEN 'female'
                ELSE 'male'
                END AS ssa_name_gender
        FROM SsaGenderWeight
    ),
    NameUsage AS (
        SELECT
            name,
            MAX(name_starting_letter) AS name_starting_letter,
            COUNT(DISTINCT state) AS name_in_states_count,
            -- the window the natality data covers, so the mart can compare
            -- like with like; named for the window it actually spans
            SUM(
                CASE
                    WHEN CAST(year AS INT) BETWEEN 1995 AND 2025 THEN name_count
                    ELSE 0
                    END) AS total_name_count_1995_2025,
            SUM(name_count) AS total_name_count
        FROM {{ ref('int_nn_baby_names') }}
        GROUP BY 1
    )
SELECT
    {{ dbt_utils.generate_surrogate_key(['U.name']) }} AS name_key,
    U.name,
    U.name_starting_letter,
    -- the origins sources describe the name itself, so they win over the SSA
    -- usage split when they have an opinion
    COALESCE(O.name_gender, G.ssa_name_gender) AS name_gender,
    O.name_gender AS name_gender_from_origins,
    G.ssa_name_gender AS name_gender_from_ssa_usage,
    G.female_count AS ssa_female_name_count,
    G.male_count AS ssa_male_name_count,
    O.origin_count,
    O.main_origin,
    O.variation_count,
    O.main_variation,
    U.name_in_states_count,
    U.total_name_count_1995_2025,
    U.total_name_count
FROM NameUsage AS U
LEFT JOIN SsaGender AS G USING(name)
LEFT JOIN {{ ref('int_nn_origins') }} AS O USING(name)
