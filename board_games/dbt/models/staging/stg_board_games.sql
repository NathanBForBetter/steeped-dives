-- One row per board game, joining the ranked-search half of the BGG pull
-- (board_game_filtered) to the per-game page half (board_game_scrape).
--
-- Every numeric column goes through mixed_int_cast() rather than a bare
-- ::float: the scraped values arrive as text and include blanks, 'N/A' and
-- the occasional scientific-notation string, and a plain cast would abort the
-- whole model on the first one. The macro regex-guards the cast so a bad
-- value becomes NULL and the other 140k rows still land.
SELECT
    CAST('{{ env_var("BOARD_GAMES_SCRAPE_DATE") }}' AS DATE) AS scrape_date,
    F.board_game_url,
    F.board_game_name,
    F.year_made,
    NULLIF(S.short_descr, '') AS short_descr,
    S.main_category_list,
    S.sub_category_list,
    S.mechanism_list,
    S.family_list,
    S.publisher_list,

    {{ mixed_int_cast('F.collection_rank', 'float') }} AS collection_rank, 
    {{ mixed_int_cast('F.geek_rating', 'float') }} AS geek_rating,
    {{ mixed_int_cast('F.avg_rating', 'float') }} AS avg_rating,
    {{ mixed_int_cast('F.voter_count', 'float') }} AS voter_count,
    {{ mixed_int_cast('S.complexity_weight', 'float') }} AS complexity_weight,
    {{ mixed_int_cast('S.min_age', 'float') }} AS min_age,
    {{ mixed_int_cast('S.min_playtime', 'float') }} AS min_playtime,
    {{ mixed_int_cast('S.max_playtime', 'float') }} AS max_playtime,
    {{ mixed_int_cast('S.min_players', 'float') }} AS min_players,
    {{ mixed_int_cast('S.max_players', 'float') }} AS max_players,

    {{ mixed_int_cast('S.honors_count', 'float') }} AS honors_count,
    {{ mixed_int_cast('S.version_count', 'float') }} AS version_count,
    {{ mixed_int_cast('S.expansion_count', 'float') }} AS expansion_count,
    {{ mixed_int_cast('S.integration_count', 'float') }} AS integration_count,
    {{ mixed_int_cast('S.cardset_count', 'float') }} AS cardset_count,
    {{ mixed_int_cast('S.accessory_count', 'float') }} AS accessory_count,

    {{ mixed_int_cast('S.page_views', 'float') }} AS page_views,
    -- combine these two into game comments,
    {{ mixed_int_cast('S.page_comments', 'float') }} AS page_comment_count,
    {{ mixed_int_cast('S.wishlist_comment_count', 'float') }} AS wishlist_comment_count,
    {{ mixed_int_cast('S.web_references', 'float') }} AS web_reference_count,
    {{ mixed_int_cast('S.podcast_references', 'float') }} AS podcast_reference_count,
    {{ mixed_int_cast('S.bgg_news_references', 'float') }} AS bgg_news_reference_count,
    -- see how distributed it is. 
    {{ mixed_int_cast('S.want_to_play_user_count', 'float') }} AS want_to_play_user_count,
    {{ mixed_int_cast('S.have_played_user_count', 'float') }} AS have_played_user_count,
    {{ mixed_int_cast('S.fan_user_count', 'float') }} AS fan_user_count,
    -- perhaps combine these three in int.
    {{ mixed_int_cast('S.on_wishlist_user_count', 'float') }} AS on_wishlist_user_count,
    {{ mixed_int_cast('S.want_to_buy_user_count', 'float') }} AS want_to_buy_user_count,
    {{ mixed_int_cast('S.wanting_user_count', 'float') }} AS wanting_user_count,
    -- combine these two.
    {{ mixed_int_cast('S.preordered_owner_count', 'float') }} AS preordered_owner_count,
    {{ mixed_int_cast('S.owner_user_count', 'float') }} AS owner_user_count,
    {{ mixed_int_cast('S.trading_user_count', 'float') }} AS trading_user_count,
    {{ mixed_int_cast('S.prev_owned_user_count', 'float') }} AS prev_owned_user_count,
    
    {{ mixed_int_cast('S.publisher_count', 'float') }} AS publisher_count,
    {{ mixed_int_cast('S.designer_count', 'float') }} AS designer_count,
    {{ mixed_int_cast('S.graphic_designer_count', 'float') }} AS graphic_designer_count,
    {{ mixed_int_cast('S.artist_count', 'float') }} AS artist_count,
    {{ mixed_int_cast('S.developer_count', 'float') }} AS developer_count
FROM {{ source('board_games_raw', 'board_game_filtered') }} AS F 
LEFT JOIN {{ source('board_games_raw', 'board_game_scrape') }} AS S USING (board_game_url)