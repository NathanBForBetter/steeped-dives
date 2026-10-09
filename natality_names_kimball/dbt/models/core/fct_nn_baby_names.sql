-- Name assignments by state, year and name gender.
--
-- Grain: state + year + name + gender, which is the grain of the SSA
-- state-level files. gender_key here plays the "name gender" role - see
-- dim_nn_gender for why that is the same dimension as the natality fact's
-- infant sex.
SELECT
    B.extract_date,
    Y.year_key,
    S.state_key,
    N.name_key,
    G.gender_key,
    B.name_count
FROM {{ ref('int_nn_baby_names') }} AS B
LEFT JOIN {{ ref('dim_nn_name') }} AS N USING(name)
LEFT JOIN {{ ref('dim_nn_year') }} AS Y USING(year)
LEFT JOIN {{ ref('dim_nn_state') }} AS S ON B.state = S.state_code
LEFT JOIN {{ ref('dim_nn_gender') }} AS G ON LOWER(B.gender) = G.gender
