-- Mother-attribute dimension: one row per (label, value) pair across the
-- four CDC WONDER breakouts: age, race, Hispanic origin and education. Race
-- and Hispanic origin each have a second label for their detailed values.
--
-- The surrogate key hashes the label and the value together. If a value such
-- as 'unknown' ever appears under two labels, each one still gets its own
-- key.
WITH
    DistinctValues AS (
        SELECT DISTINCT
            mother_attr_label,
            mother_attr_value
        FROM {{ ref('int_nn_natality') }}
        WHERE mother_attr_value IS NOT NULL
    )
SELECT
    {{ dbt_utils.generate_surrogate_key(['mother_attr_label', 'mother_attr_value']) }}
        AS mother_key,
    mother_attr_label,
    mother_attr_value
FROM DistinctValues
