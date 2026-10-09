-- Births by state, year, infant sex and one mother attribute.
--
-- Grain: state + year + infant_gender + mother_attr_label + mother_attr_value,
-- the grain int_nn_natality produces. gender_key here means the infant's sex
-- at birth; see dim_nn_gender.
SELECT
    Y.year_key,
    S.state_key,
    M.mother_key,
    G.gender_key,
    B.extract_date,
    B.mother_attr_birth_count,
    -- all births in the state, year and infant sex, so any attribute count
    -- can be read as a share of all births
    B.total_birth_count,
    -- the CDC WONDER vintage the count came from, kept on the fact so any
    -- number can be traced to its source file
    B.source AS wonder_vintage
FROM {{ ref('int_nn_natality') }} AS B
LEFT JOIN {{ ref('dim_nn_year') }} AS Y USING(year)
-- The natality side carries full state names ('Alabama', 'District of
-- Columbia') and the names side carries two-letter codes. dim_nn_state holds
-- both, so each fact joins on the one its source has.
LEFT JOIN {{ ref('dim_nn_state') }} AS S ON B.state = S.state_name
LEFT JOIN {{ ref('dim_nn_mother') }} AS M
    USING(mother_attr_label, mother_attr_value)
LEFT JOIN {{ ref('dim_nn_gender') }} AS G ON LOWER(B.infant_gender) = G.gender
