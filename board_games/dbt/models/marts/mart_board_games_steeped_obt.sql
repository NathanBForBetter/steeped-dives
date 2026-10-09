-- One Big Table for the Steeped AI platform.
--
-- Grain: one row per board game. The first column packs the url, name and
-- description into a single labeled text block because that is what the
-- platform profiles as the entity under analysis; everything after it is a
-- typed attribute or measure, so cross-tabs and regressions need no joins.
SELECT
    CONCAT(F.board_game_url,
        CASE
            WHEN F.board_game_name IS NOT NULL
                THEN CONCAT(E'\n Name: ', F.board_game_name)
                ELSE ''
                END,
        CASE
            WHEN F.short_descr IS NOT NULL
                THEN CONCAT(E'\n Description: ', F.short_descr)
            ELSE ''
            END) AS board_game,
    -- which BGG scrape these numbers came from, so a published Steeped
    -- Dive is reproducible against one point in time
    F.scrape_date,
    Y.year_made_int AS year_made,
    Y.decade,
    Y.era,
    Y.is_unknown_year,
    F.main_publisher,
    F.secondary_publisher,

    F.history_war_type,
    F.history_war_cat,
    F.fantasy_adventure_type,
    F.fantasy_adventure_cat,
    F.skill_type,
    F.skill_cat,
    F.attribute_type,
    F.attribute_cat,
    F.component_type,
    F.component_cat,
    F.politics_gov_type,
    F.politics_gov_cat,
    F.pop_culture_type,
    F.pop_culture_cat,
    F.crime_violence_type,
    F.crime_violence_cat,
    F.industry_occupation_type,
    F.industry_occupation_cat,
    F.creature_animal_type,
    F.creature_animal_cat,
    F.transportation_travel_type,
    F.transportation_travel_cat,
    F.sport_type,
    F.sport_cat,
    F.player_audience_type,
    F.player_audience_cat,

    F.collection_rank,
    F.geek_rating,
    F.avg_rating,
    F.voter_count,
    F.complexity_weight,
    F.min_age,
    F.min_playtime,
    F.max_playtime,
    F.min_players,
    F.max_players,
    F.honors_count,
    F.version_count,
    F.expansion_count,
    F.integration_count,
    F.cardset_count,
    F.accessory_count,
    F.page_views,
    F.game_comment_count,
    F.web_reference_count,
    F.podcast_reference_count,
    F.bgg_news_reference_count,
    F.publisher_count,
    F.designer_count,
    F.graphic_designer_count,
    F.artist_count,
    F.developer_count,

    F.want_to_play_user_count,
    F.on_wishlist_user_count,
    F.have_played_user_count,
    F.fan_user_count,
    F.want_to_buy_user_count,
    F.wanting_user_count,
    F.preordered_owner_count,
    F.owner_user_count,
    F.trading_user_count,
    F.prev_owned_user_count,
    F.stage_consideration_user_count,
    F.stage_engagement_user_count,
    F.stage_purchase_intent_user_count,
    F.stage_ownership_user_count,
    F.stage_churn_user_count
FROM {{ ref('fct_board_games') }} AS F
LEFT JOIN {{ ref('dim_board_games_year') }} AS Y USING(year_key)
