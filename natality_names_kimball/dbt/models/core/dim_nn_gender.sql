-- Conformed gender dimension, played as two different roles.
--
-- This is the subtlest thing in the project and it is worth being explicit
-- about. The same dimension is referenced by both facts, but it means
-- different things in each:
--
--   fct_nn_baby_names.gender_key      -> the gender SSA recorded for the NAME
--   fct_nn_baby_natality.gender_key   -> the infant's SEX AT BIRTH per CDC
--
-- That is a textbook role-playing dimension, and the whole point of the mart
-- is to compare the two. Keeping one dimension with one key domain is what
-- makes the comparison a join rather than a reconciliation; the facts carry
-- the role in their own column semantics, documented in _core__models.yml.
--
-- Built from the union of both sources so neither fact can produce a value
-- the dimension has not got a member for.
WITH
    NatalityGenders AS (
        SELECT DISTINCT LOWER(infant_gender) AS gender
        FROM {{ ref('int_nn_natality') }}
        WHERE infant_gender IS NOT NULL
    ),
    NameGenders AS (
        SELECT DISTINCT LOWER(gender) AS gender
        FROM {{ ref('int_nn_baby_names') }}
        WHERE gender IS NOT NULL
    ),
    AllGenders AS (
        SELECT gender FROM NatalityGenders
        UNION
        SELECT gender FROM NameGenders
    )
SELECT
    {{ dbt_utils.generate_surrogate_key(['gender']) }} AS gender_key,
    gender
FROM AllGenders
