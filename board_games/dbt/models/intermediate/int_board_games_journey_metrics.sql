-- Board game shopping journey metrics: the BGG user counts as reported, plus
-- the five funnel stages they add up to. Each stage is the sum of the two
-- counts listed under it, and the counts themselves are passed through
-- unchanged.
SELECT
    board_game_url,

    -- consideration
    want_to_play_user_count,
    on_wishlist_user_count,

    -- engagement
    have_played_user_count,
    fan_user_count,

    -- purchase intent
    want_to_buy_user_count,
    wanting_user_count,

    -- ownership
    preordered_owner_count,
    owner_user_count,

    -- churn
    trading_user_count,
    prev_owned_user_count,

    want_to_play_user_count
        + on_wishlist_user_count AS stage_consideration_user_count,
    have_played_user_count + fan_user_count AS stage_engagement_user_count,
    want_to_buy_user_count + wanting_user_count AS stage_purchase_intent_user_count,
    preordered_owner_count + owner_user_count AS stage_ownership_user_count,
    trading_user_count + prev_owned_user_count AS stage_churn_user_count
FROM {{ ref('stg_board_games') }}
