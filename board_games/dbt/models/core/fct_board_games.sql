-- One row per board game: the conformed year key, 13 category keys with their
-- text, and every metric and journey measure.
--
-- Descriptive attributes (name, publishers, year, description) are read from
-- the scd_board_game snapshot as of scrape_date, not from the intermediate
-- model directly. With one scrape taken the two are the same thing. The join
-- is in place so that history is captured from the first scrape onward.
--
-- Nothing in here aggregates. Every input is one row per board_game_url, so a
-- join that ever matched twice shows up as a duplicate key and fails the
-- grain test, which is the point.
WITH
    DimJoin AS (
        SELECT
            I.board_game_url,
            I.scrape_date,
            V.year_made_int,
            V.board_game_name,
            V.short_descr,
            V.main_publisher,
            V.secondary_publisher,
            CASE WHEN C.history_war_cat IS NOT NULL
                 THEN {{ dbt_utils.generate_surrogate_key(['C.history_war_cat']) }}
                 END AS history_key,
            C.history_war_type,
            C.history_war_cat,
            C.history_war_source,
            CASE WHEN C.fantasy_adventure_cat IS NOT NULL
                 THEN {{ dbt_utils.generate_surrogate_key(['C.fantasy_adventure_cat']) }}
                 END AS fantasy_key,
            C.fantasy_adventure_type,
            C.fantasy_adventure_cat,
            C.fantasy_adventure_source,
            CASE WHEN C.skill_cat IS NOT NULL
                 THEN {{ dbt_utils.generate_surrogate_key(['C.skill_cat']) }}
                 END AS skill_key,
            C.skill_type,
            C.skill_cat,
            C.skill_source,
            CASE WHEN C.attribute_cat IS NOT NULL
                 THEN {{ dbt_utils.generate_surrogate_key(['C.attribute_cat']) }}
                 END AS attribute_key,
            C.attribute_type,
            C.attribute_cat,
            C.attribute_source,
            CASE WHEN C.component_cat IS NOT NULL
                 THEN {{ dbt_utils.generate_surrogate_key(['C.component_cat']) }}
                 END AS component_key,
            C.component_type,
            C.component_cat,
            C.component_source,
            CASE WHEN C.politics_gov_cat IS NOT NULL
                 THEN {{ dbt_utils.generate_surrogate_key(['C.politics_gov_cat']) }}
                 END AS politics_key,
            C.politics_gov_type,
            C.politics_gov_cat,
            C.politics_gov_source,
            CASE WHEN C.pop_culture_cat IS NOT NULL
                 THEN {{ dbt_utils.generate_surrogate_key(['C.pop_culture_cat']) }}
                 END AS pop_culture_key,
            C.pop_culture_type,
            C.pop_culture_cat,
            C.pop_culture_source,
            CASE WHEN C.crime_violence_cat IS NOT NULL
                 THEN {{ dbt_utils.generate_surrogate_key(['C.crime_violence_cat']) }}
                 END AS crime_key,
            C.crime_violence_type,
            C.crime_violence_cat,
            C.crime_violence_source,
            CASE WHEN C.industry_occupation_cat IS NOT NULL
                 THEN {{ dbt_utils.generate_surrogate_key(['C.industry_occupation_cat']) }}
                 END AS industry_key,
            C.industry_occupation_type,
            C.industry_occupation_cat,
            C.industry_occupation_source,
            CASE WHEN C.creature_animal_cat IS NOT NULL
                 THEN {{ dbt_utils.generate_surrogate_key(['C.creature_animal_cat']) }}
                 END AS creature_key,
            C.creature_animal_type,
            C.creature_animal_cat,
            C.creature_animal_source,
            CASE WHEN C.transportation_travel_cat IS NOT NULL
                 THEN {{ dbt_utils.generate_surrogate_key(['C.transportation_travel_cat']) }}
                 END AS transportation_key,
            C.transportation_travel_type,
            C.transportation_travel_cat,
            C.transportation_travel_source,
            CASE WHEN C.sport_cat IS NOT NULL
                 THEN {{ dbt_utils.generate_surrogate_key(['C.sport_cat']) }}
                 END AS sport_key,
            C.sport_type,
            C.sport_cat,
            C.sport_source,
            
            CASE WHEN C.player_audience_cat IS NOT NULL
                 THEN {{ dbt_utils.generate_surrogate_key(['C.player_audience_cat']) }}
                 END AS player_key,
            C.player_audience_type,
            C.player_audience_cat,
            C.player_audience_source
        FROM {{ ref('int_board_games_main_dims') }} AS I
        -- Point-in-time join: the version of the game that was in force on
        -- the scrape date. dbt_valid_from is inclusive and dbt_valid_to is
        -- exclusive, so >= and < here. Using <= on both would match two
        -- versions on the day one replaced the other.
        LEFT JOIN {{ ref('scd_board_game') }} AS V
            ON  V.board_game_url = I.board_game_url
            AND I.scraped_at >= V.dbt_valid_from
            AND (I.scraped_at < V.dbt_valid_to OR V.dbt_valid_to IS NULL)
            AND V.dbt_is_deleted = 'False'
        LEFT JOIN {{ ref('int_board_games_categories') }} AS C
            ON C.board_game_url = I.board_game_url
    )
SELECT
    D.*,
    -- Never null: a game with no usable year points at the dimension's
    -- explicit 'unknown' member.
    COALESCE(Y.year_key, {{ dbt_utils.generate_surrogate_key(["'unknown'"]) }}) AS year_key,

    M.collection_rank, 
    M.geek_rating,
    M.avg_rating,
    M.voter_count,
    M.complexity_weight,
    M.min_age,
    M.min_playtime,
    M.max_playtime,
    M.min_players,
    M.max_players,
    M.honors_count,
    M.version_count,
    M.expansion_count,
    M.integration_count,
    M.cardset_count,
    M.accessory_count,
    M.page_views,
    M.game_comment_count,
    M.web_reference_count,
    M.podcast_reference_count,
    M.bgg_news_reference_count,
    M.publisher_count,
    M.designer_count,
    M.graphic_designer_count,
    M.artist_count,
    M.developer_count,

    J.want_to_play_user_count,
    J.on_wishlist_user_count,
    J.have_played_user_count,
    J.fan_user_count,
    J.want_to_buy_user_count,
    J.wanting_user_count,
    J.preordered_owner_count,
    J.owner_user_count,
    J.trading_user_count,
    J.prev_owned_user_count,
    J.stage_consideration_user_count,
    J.stage_engagement_user_count,
    J.stage_purchase_intent_user_count,
    J.stage_ownership_user_count,
    J.stage_churn_user_count
FROM DimJoin AS D
LEFT JOIN {{ ref('int_board_games_main_metrics') }} AS M USING(board_game_url)
LEFT JOIN {{ ref('int_board_games_journey_metrics') }} AS J USING(board_game_url)
LEFT JOIN {{ ref('dim_board_games_year') }} AS Y USING(year_made_int)