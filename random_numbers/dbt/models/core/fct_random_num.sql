-- One row per number and source: how often that source picked that number.
-- Every source has all 100 numbers, with a count of 0 where it never picked
-- one.
SELECT
    N.number_key,
    C.source,
    C.dl_date,
    C.choice_count_share,
    C.choice_count,
    C.total_source_choice_count
FROM {{ ref('int_random_num_combine') }} AS C
LEFT JOIN {{ ref('dim_random_num_chosen') }} AS N USING(number_chosen)
