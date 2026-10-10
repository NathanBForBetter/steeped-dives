-- Number dimension: one row for each number from 1 to 100, with the
-- attributes the pattern mart groups by.
WITH 
    DistinctValues AS (
        SELECT DISTINCT
            number_chosen,
            number_tens_range,
            number_is_even,
            number_is_odd,
            number_last_digit
        FROM {{ ref('int_random_num_combine') }}
    )
SELECT 
    {{ dbt_utils.generate_surrogate_key(['number_chosen']) }} AS number_key,
    number_chosen,
    number_tens_range,
    number_is_even,
    number_is_odd,
    number_last_digit
FROM DistinctValues
