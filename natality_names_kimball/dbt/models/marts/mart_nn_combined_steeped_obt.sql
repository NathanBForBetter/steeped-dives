-- One Big Table joining the two facts for the Steeped AI platform.
--
-- GRAIN: name + year_state_name_gender + state + year + mother_attr_label
--        + mother_attr_value.
--
-- The SSA counts boys and girls separately, so a name given to both in a
-- state and year has two rows on the names side. year_state_name_gender says
-- which of the two each row is.
--
-- The join from the names side to the natality aggregate is a deliberate
-- many-to-many on (state_key, year_key): each names row appears once per
-- mother-attribute value in that state and year. That cross product is the
-- output. It lets the platform cross-tabulate name choice against mother age,
-- race, Hispanic origin and education without being told which joins to make
-- or which grain each column sits at.
--
-- Measures from the names side therefore repeat across the mother-attribute
-- rows, and measures from the natality side repeat across names. Steeped AI
-- resolves that at aggregation time rather than assuming one row is one
-- observation, which is the reason the table is shaped this way. Anything
-- aggregating this table by hand should group to the grain above first.
WITH
    BabyNatalityAgg AS (
        SELECT
            F.year_key,
            F.state_key,
            F.extract_date,
            M.mother_attr_label AS year_state_nat_mother_label,
            M.mother_attr_value AS year_state_nat_mother_val,

            -- births for THIS mother-attribute value, both sexes
            SUM(F.mother_attr_birth_count) AS year_state_nat_mother_val_birth_count,

            -- all births in the state/year, independent of the attribute.
            -- MAX not SUM: total_birth_count is already a state/year/gender
            -- total repeated on every attribute row, so summing it over the
            -- group would multiply it by the number of sexes present.
            MAX(F.total_birth_count) FILTER (WHERE G.gender = 'female')
                + MAX(F.total_birth_count) FILTER (WHERE G.gender = 'male')
                AS year_state_nat_total_birth_count,

            MAX(F.total_birth_count) FILTER (WHERE G.gender = 'female')
                AS year_state_nat_female_birth_count,
            MAX(F.total_birth_count) FILTER (WHERE G.gender = 'male')
                AS year_state_nat_male_birth_count
        FROM {{ ref('fct_nn_baby_natality') }} AS F
        LEFT JOIN {{ ref('dim_nn_mother') }} AS M USING(mother_key)
        LEFT JOIN {{ ref('dim_nn_gender') }} AS G USING(gender_key)
        GROUP BY 1, 2, 3, 4, 5
    ),

    -- The names side at its own grain, before the natality join. Everything
    -- that has to be computed over names lives here, so it is computed once
    -- on one row per name/gender/state/year rather than on the fanned-out
    -- product.
    NameGrain AS (
        SELECT
            F.extract_date,
            F.name_key,
            F.state_key,
            F.year_key,
            N.name,
            N.name_starting_letter,
            N.name_gender,
            N.name_gender_from_origins,
            N.name_gender_from_ssa_usage,
            N.origin_count,
            N.main_origin AS main_name_origin,
            N.variation_count,
            N.main_variation AS main_name_variation,
            N.total_name_count_1995_2025 AS name_count_1995_2025_all_states,
            S.state_name,
            S.state_starting_letter,
            S.region,
            Y.year,
            Y.year_int,
            Y.decade,
            Y.generation,
            -- the sex the SSA recorded for these babies. name_gender above
            -- describes the name overall, so the two can differ.
            G.gender AS year_state_name_gender,
            F.name_count AS year_state_name_count
        FROM {{ ref('fct_nn_baby_names') }} AS F
        LEFT JOIN {{ ref('dim_nn_name') }} AS N USING(name_key)
        LEFT JOIN {{ ref('dim_nn_gender') }} AS G USING(gender_key)
        LEFT JOIN {{ ref('dim_nn_state') }} AS S USING(state_key)
        LEFT JOIN {{ ref('dim_nn_year') }} AS Y USING(year_key)
        -- On par with natality details
        WHERE Y.year_int BETWEEN 1995 AND 2025
    ),

    -- Name-gender split per state/year. A property of names alone, so it is
    -- derived from NameGrain and not from the fanned-out output. No
    -- de-duplication is needed, and the ratio does not depend on every name
    -- in a state/year repeating the same number of times.
    YearStateNameGenderShare AS (
        SELECT
            year_key,
            state_key,
            {{ safe_divide(
                "SUM(CASE WHEN name_gender = 'female' THEN year_state_name_count END)",
                'SUM(year_state_name_count)') }} AS year_state_name_female_share,
            {{ safe_divide(
                "SUM(CASE WHEN name_gender = 'male' THEN year_state_name_count END)",
                'SUM(year_state_name_count)') }} AS year_state_name_male_share,
            SUM(year_state_name_count) AS year_state_name_total_count
        FROM NameGrain
        GROUP BY 1, 2
    )

-- The fan-out happens here, once, and the surrogate keys stay out of the
-- output: they are 64-character hashes with no analytical meaning, and the
-- platform would otherwise profile them as columns.
SELECT
    -- from the names side, which drives this join, so it is never null
    N.extract_date,

    N.name,
    N.name_starting_letter,
    N.name_gender,
    N.name_gender_from_origins,
    N.name_gender_from_ssa_usage,
    N.origin_count,
    N.main_name_origin,
    N.variation_count,
    N.main_name_variation,

    N.state_name,
    N.state_starting_letter,
    N.region,

    N.year,
    N.decade,
    N.generation,

    B.year_state_nat_mother_label,
    B.year_state_nat_mother_val,

    N.year_state_name_gender,
    N.year_state_name_count,
    N.name_count_1995_2025_all_states,
    G.year_state_name_total_count,
    G.year_state_name_female_share,
    G.year_state_name_male_share,

    B.year_state_nat_mother_val_birth_count,
    B.year_state_nat_total_birth_count,

    -- share of all births in the state/year that were female / male. The
    -- denominator is the state/year total.
    {{ safe_divide('B.year_state_nat_female_birth_count',
                   'B.year_state_nat_total_birth_count') }}
        AS year_state_nat_female_birth_share,
    {{ safe_divide('B.year_state_nat_male_birth_count',
                   'B.year_state_nat_total_birth_count') }}
        AS year_state_nat_male_birth_share,

    -- share of births in the state/year carrying THIS mother attribute
    -- value, out of all births in that state/year
    {{ safe_divide('B.year_state_nat_mother_val_birth_count',
                   'B.year_state_nat_total_birth_count') }}
        AS year_state_nat_mother_val_birth_share
FROM NameGrain AS N
LEFT JOIN BabyNatalityAgg AS B USING(state_key, year_key)
LEFT JOIN YearStateNameGenderShare AS G USING(state_key, year_key)
