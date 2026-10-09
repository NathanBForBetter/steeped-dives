SELECT
    board_game_url,
    collection_rank, 
    geek_rating,
    avg_rating,
    voter_count,
    complexity_weight,
    min_age,
    min_playtime,
    max_playtime,
    min_players,
    max_players,

    honors_count,
    version_count,
    expansion_count,
    integration_count,
    cardset_count,
    accessory_count,

    page_views,
    page_comment_count + wishlist_comment_count AS game_comment_count,
    web_reference_count,
    podcast_reference_count,
    bgg_news_reference_count,

    publisher_count,
    designer_count,
    graphic_designer_count,
    artist_count,
    developer_count
FROM {{ ref('stg_board_games') }}