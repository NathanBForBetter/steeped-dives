-- BehindTheName origins, one row per source row. A name can appear more than
-- once, and int_nn_origins resolves that.
--
-- The name comes from `names_clean`. extract_origins_btn.py folds accents to
-- plain ASCII with unaccent() so that José, Zoë and Renée line up with the
-- SSA file, which carries no accents. The accented original is kept as
-- name_raw.
SELECT
    CAST('{{ env_var("NATALITY_EXTRACT_DATE") }}' AS DATE) AS extract_date,
    LOWER(names_clean) AS name,
    LOWER(nll) AS name_raw,
    LOWER(usg) AS origin,
    LOWER(usg_2) AS origin_2,
    LOWER(nl) AS variant_of,
    fem AS female,
    masc AS male
FROM {{ source('national_names', 'nn_origins_raw') }}
WHERE names_clean IS NOT NULL
