-- Turns four sparse BGG list columns (sub_category, mechanism, family,
-- main_category) into 13 category families. Each family has a _type (the
-- specific term), a _cat (the bucket it rolls up to) and a _source (which of
-- the four lists the value came from).
--
-- The IN lists below were not picked by hand. The distinct terms came from
-- int_board_games_category_vocabulary, were embedded with a sentence
-- transformer and clustered, and the clusters are what the lists hold. Both
-- models build their keys with format_category(), so a term has the same key
-- here as it had when it was clustered.
--
-- The four lists are merged in priority order: sub_category, then mechanism,
-- then family, then main_category. The most specific list wins and _source
-- records which one that was.
WITH
    -- Every game gets a row, including games with no sub-category, so their
    -- mechanism, family and main category values are still picked up.
    GameSpine AS (
        SELECT board_game_url
        FROM {{ ref('stg_board_games') }}
    ),
    SubCatRaw AS (
        SELECT
            board_game_url,
            sub_category AS category_raw,
            {{ format_category('sub_category') }} AS category_formatted
        FROM {{ ref('stg_board_games') }}, UNNEST(sub_category_list) AS sub_category
    ),
    SubCat AS (
        SELECT
            board_game_url,
            -- can be bool or high level cat
            MAX(
                CASE
                    WHEN category_formatted IN (
                        'war', 'vietnam war', 'american revolutionary war', 'american indian wars', 
                        'world war ii', 'modern warfare', 'civil war', 'korean war', 'american civil war', 
                        'world war i')
                        THEN 'war'
                    WHEN category_formatted IN (
                        'ancient', 'age of reason', 'prehistoric', 'civilization', 'american west',
                        'religious', 'renaissance', 'post napoleonic', 'medieval', 'napoleonic', 
                        'arabian')
                        THEN 'history'
                    END) AS history_war_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN ( 'war', 'vietnam war', 'american revolutionary war', 'american indian wars', 
                        'world war ii', 'modern warfare', 'civil war', 'korean war', 'american civil war', 
                        'world war i', 
                        'ancient', 'age of reason', 'prehistoric', 'civilization', 'american west',
                        'religious', 'renaissance', 'post napoleonic', 'medieval','napoleonic', 
                        'arabian')
                        THEN category_formatted
                    END) AS history_war_type,
            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'mythology', 'zombies', 'fantasy', 'science fiction')
                        THEN 'fantasy'
                    WHEN category_formatted IN (
                        'adventure', 'exploration', 'space exploration')
                        THEN 'adventure'
                    END) AS fantasy_adventure_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'mythology', 'zombies', 'fantasy', 'science fiction', 'adventure', 'exploration', 'space exploration')
                        THEN category_formatted
                    END) AS fantasy_adventure_type,

            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'abstract strategy', 'territory building',  'city building', 'bluffing', 
                        'negotiation', 'memory', 'action or dexterity', 'deduction', 'number', 
                        'trivia', 'humor', 'puzzle', 'maze', 'math', 'word', 'educational')
                        THEN 'skill'
                    END) AS skill_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'abstract strategy', 'territory building',  'city building', 'bluffing', 
                        'negotiation', 'memory', 'action or dexterity', 'deduction', 'number', 
                        'trivia', 'humor', 'puzzle', 'maze', 'math', 'word', 'educational')
                        THEN category_formatted
                    END) AS skill_type,

            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'fan expansion', 'expansion for base', 'third party expansion')
                        THEN 'expansion'
                    WHEN category_formatted IN (
                        'print & play', 'system', 'real time', 'electronic' )
                        THEN 'mechanism'
                    END) AS attribute_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'fan expansion', 'expansion for base', 'third party expansion',
                        'print & play', 'system', 'real time', 'electronic'
                        )
                        THEN category_formatted
                    END) AS attribute_type,

            MAX(
                CASE 
                    WHEN category_formatted IN ('dice', 'collectible components', 'card', 'miniatures')
                        THEN 'component'
                    END) AS component_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN ('dice', 'collectible components', 'card', 'miniatures')
                        THEN category_formatted
                    END) AS component_type,

            MAX(
                CASE 
                    WHEN category_formatted IN ('environmental', 'political')
                        THEN 'politics'
                    WHEN category_formatted IN (
                        'spies or secret agents')
                        THEN 'government'
                    END) AS politics_gov_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN ('environmental', 'political', 'spies or secret agents')
                        THEN category_formatted
                    END) AS politics_gov_type,

            MAX(
                CASE 
                    WHEN category_formatted IN ('book', 'comic book or strip', 'novel based')
                        THEN 'literary'
                    WHEN category_formatted IN (
                        'video theme', 'movies or tv or radio theme', 'music')
                        THEN 'media'
                    END) AS pop_culture_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'book', 'comic book or strip', 'novel based',
                        'video theme', 'movies or tv or radio theme', 'music')
                        THEN category_formatted
                    END) AS pop_culture_type,

            MAX(
                CASE 
                    WHEN category_formatted IN ('mafia', 'pirates', 'murder or mystery')
                        THEN 'crime'
                    WHEN category_formatted IN ('pike and shot', 'fighting', 'horror')
                        THEN 'violence'
                    END) AS crime_violence_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'mafia', 'pirates', 'murder or mystery', 'pike and shot', 'fighting', 'horror')
                        THEN category_formatted
                    END) AS crime_violence_type,
            
            MAX(
                CASE 
                    WHEN category_formatted IN ('industry or manufacturing', 'farming', 'medical')
                        THEN 'industry'
                    WHEN category_formatted IN ('economic')
                        THEN 'economic'
                    END) AS industry_occupation_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN ('industry or manufacturing', 'economic', 'farming', 'medical')
                        THEN category_formatted
                    END) AS industry_occupation_type,

            MAX(
                CASE 
                    WHEN category_formatted IN ('animals')
                        THEN 'animal'
                    END) AS creature_animal_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN ('animals')
                        THEN category_formatted
                    END) AS creature_animal_type,

            MAX(
                CASE 
                    WHEN category_formatted IN ('aviation or flight', 'trains', 'transportation')
                        THEN 'transportation'
                    WHEN category_formatted IN ('travel')
                        THEN 'travel'
                    END) AS transportation_travel_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'aviation or flight', 'trains', 'transportation', 'travel')
                        THEN category_formatted
                    END) AS transportation_travel_type,

            MAX(
                CASE 
                    WHEN category_formatted IN ('sports', 'racing')
                        THEN 'sport'
                    END) AS sport_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN ('sports', 'racing')
                        THEN category_formatted
                    END) AS sport_type,

            MAX(
                CASE 
                    WHEN category_formatted IN ('children''s', 'party', 'mature or adult')
                        THEN 'audience'
                    END) AS player_audience_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN ('children''s', 'party', 'mature or adult')
                        THEN category_formatted
                    END) AS player_audience_type      
        FROM SubCatRaw
        GROUP BY 1
    ),
    MechRaw AS (
        SELECT
            board_game_url,
            mechanism AS category_raw,
            {{ format_category('mechanism') }} AS category_formatted
        FROM {{ ref('stg_board_games') }}, UNNEST(mechanism_list) AS mechanism
    ),
    Mech AS (
        SELECT
            board_game_url,
            -- can be bool or high level cat
            MAX(NULL::text) AS history_war_cat,
            MAX(NULL::text) AS history_war_type,
            MAX(NULL::text) AS fantasy_adventure_cat,
            MAX(NULL::text) AS fantasy_adventure_type,

            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'cooperative', 'negotiation', 'ownership', 'semi cooperative', 'alliances',  
                        'mancala', 'pattern building', 'pattern recognition', 'speed matching', 'matching',
                        'follow', 'ordering', 'singing', 'chaining', 'storytelling', 'memory', 'questions and answers',
                        'acting', 'bingo', 'spelling', 'king of the hill', 'rock paper scissors', 'trick taking', 'ladder climbing', 
                        'catch the leader', 're rolling and locking', 'impulse movement', 'area movement', 'three dimensional movement',
                        'programmed movement', 'hidden movement', 'track movement', 'pattern movement', 'point to point movement',
                        'grid movement', 'drawing', 'open drafting', 'paper and pencil', 'line drawing', 'closed drafting', 'deduction',
                        'stat check resolution', 'tech trees or tech tracks', 'order counters', 'connections', 'narrative choice or paragraph',
                        'relative movement', 'measurement movement', 'movement template', 'slide or push', 'movement points', 'stacking and balancing', 
                        'moving multiple units', 'hand management'
                        )
                        THEN 'skill'
                    END) AS skill_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'cooperative', 'negotiation', 'ownership', 'semi cooperative', 'alliances',  
                        'mancala', 'pattern building', 'pattern recognition', 'speed matching', 'matching',
                        'follow', 'ordering', 'singing', 'chaining', 'storytelling', 'memory', 'questions and answers',
                        'acting', 'bingo', 'spelling', 'king of the hill', 'rock paper scissors', 'trick taking', 'ladder climbing', 
                        'catch the leader', 're rolling and locking', 'impulse movement', 'area movement', 'three dimensional movement',
                        'programmed movement', 'hidden movement', 'track movement', 'pattern movement', 'point to point movement',
                        'grid movement', 'drawing', 'open drafting', 'paper and pencil', 'line drawing', 'closed drafting', 'deduction',
                        'stat check resolution', 'tech trees or tech tracks', 'order counters', 'connections', 'narrative choice or paragraph',
                        'relative movement', 'measurement movement', 'movement template', 'slide or push', 'movement points', 'stacking and balancing', 
                        'moving multiple units', 'hand management'
                        )
                        THEN category_formatted
                    END) AS skill_type,

            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'variable phase order', 'turn order time track', 'turn order role order', 'turn order claim action', 'turn order progressive', 'lose a turn', 'turn order random',
                        'turn order stat based', 'turn order pass order', 'random production', 'algorithmic resolution',
                        'automatic resource growth', 'resource queue', 'score and reset', 'pick up and deliver', 'layering', 
                        'take that', 'variable set up', 'simulation', 'events', 'force commitment', 'elapsed real time ending', 
                        'static capture', 'finale ending', 'hot potato', 'interrupts', 'flicking', 'real time', 'sudden death ending',
                        'variable player powers', 'role playing', 'roles with asymmetric information', 'hidden roles',
                        'secret unit deployment', 'targeted clues', 'single play', 'once per abilities', 'induction',
                        'highest lowest scoring', 'rondel', 'legacy', 'voting', 'traitor', 'i cut, you choose', 'bribery', 'tug of war',
                        'push your luck', 'passed action token', 'expiring actions', 'action retrieval', 'action or event', 'simultaneous action selection',
                        'action points', 'action queue', 'action timer', 'action drafting', 'neighbor scope', 'line of sight',
                        'facing', 'physical removal', 'chit pull system', 'bias','enclosure'
                        )

                        THEN 'mechanism'
                    END) AS attribute_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'variable phase order', 'turn order time track', 'turn order role order', 'turn order claim action', 'turn order progressive', 'lose a turn', 'turn order random',
                        'turn order stat based', 'turn order pass order', 'random production', 'algorithmic resolution',
                        'automatic resource growth', 'resource queue', 'score and reset', 'pick up and deliver', 'layering', 
                        'take that', 'variable set up', 'simulation', 'events', 'force commitment', 'elapsed real time ending', 
                        'static capture', 'finale ending', 'hot potato', 'interrupts', 'flicking', 'real time', 'sudden death ending',
                        'variable player powers', 'role playing', 'roles with asymmetric information', 'hidden roles',
                        'secret unit deployment', 'targeted clues', 'single play', 'once per abilities', 'induction',
                        'highest lowest scoring', 'rondel', 'legacy', 'voting', 'traitor', 'i cut, you choose', 'bribery', 'tug of war',
                        'push your luck', 'passed action token', 'expiring actions', 'action retrieval', 'action or event', 'simultaneous action selection',
                        'action points', 'action queue', 'action timer', 'action drafting', 'neighbor scope', 'line of sight',
                        'facing', 'physical removal', 'chit pull system', 'bias','enclosure'
                        )
                        THEN category_formatted
                    END) AS attribute_type,

            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'hidden victory points', 'advantage token', 'end bonuses', 'resource to move', 'victory points as a resource',
                        'contracts', 'dice rolling', 'different dice movement', 'deck, bag, and pool building', 
                        'modular board', 'campaign or battle card driven', 'move through deck', 'command cards', 'deck construction', 
                        'multi use cards', 'card play conflict resolution', 'die icon resolution', 'set collection', 'tags',
                        'scenario or mission or campaign', 'roll or spin and move', 'square grid', 'worker placement, different worker types', 
                        'cube tower', 'worker placement with dice workers', 'worker placement', 'hexagon grid', 'tile placement',
                        'area majority or influence', 'zone of control', 'grid coverage', 'area impulse', 'visual restriction',
                        'map addition', 'map reduction', 'minimap resolution', 'multiple maps', 'map deformation', 'pieces as map')
                        THEN 'component'
                    END) AS component_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'hidden victory points', 'advantage token', 'end bonuses', 'resource to move', 'victory points as a resource',
                        'contracts', 'dice rolling', 'different dice movement', 'deck, bag, and pool building', 
                        'modular board', 'campaign or battle card driven', 'move through deck', 'command cards', 'deck construction', 
                        'multi use cards', 'card play conflict resolution', 'die icon resolution', 'set collection', 'tags',
                        'scenario or mission or campaign', 'roll or spin and move', 'square grid', 'worker placement, different worker types', 
                        'cube tower', 'worker placement with dice workers', 'worker placement', 'hexagon grid', 'tile placement',
                        'area majority or influence', 'zone of control', 'grid coverage', 'area impulse', 'visual restriction',
                        'map addition', 'map reduction', 'minimap resolution', 'multiple maps', 'map deformation', 'pieces as map')
                        THEN category_formatted
                    END) AS component_type,

            MAX(NULL::text) AS politics_gov_cat,
            MAX(NULL::text) AS politics_gov_type,

            MAX(NULL::text) AS pop_culture_cat,
            MAX(NULL::text) AS pop_culture_type,

            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'ratio or combat results table', 'melding and splaying', 'critical hits and failures',
                        'lane battler', 'kill steal')
                        THEN 'violence'
                    END) AS crime_violence_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'ratio or combat results table', 'melding and splaying', 'critical hits and failures',
                        'lane battler', 'kill steal')
                        THEN category_formatted
                    END) AS crime_violence_type,
            
            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'closed economy auction', 'turn order auction', 'auction compensation', 'auction or bidding',
                        'auction dexterity', 'constrained bidding', 'auction once around', 'auction fixed placement',
                        'auction multiple lot', 'auction dutch', 'auction dutch priority', 'auction sealed bid', 'auction english',
                        'selection order bid', 'predictive bid', 'bids as wagers', 'commodity speculation', 'betting and bluffing',
                        'stock holding', 'trading', 'income', 'investment', 'loans', 'market', 'delayed purchase', 'increase value of unchosen resources',
                        'auction turn order until pass')
                        THEN 'economic'
                    END) AS industry_occupation_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'closed economy auction', 'turn order auction', 'auction compensation', 'auction or bidding',
                        'auction dexterity', 'constrained bidding', 'auction once around', 'auction fixed placement',
                        'auction multiple lot', 'auction dutch', 'auction dutch priority', 'auction sealed bid', 'auction english',
                        'selection order bid', 'predictive bid', 'bids as wagers', 'commodity speculation', 'betting and bluffing',
                        'stock holding', 'trading', 'income', 'investment', 'loans', 'market', 'delayed purchase', 'increase value of unchosen resources',
                        'auction turn order until pass')
                        THEN category_formatted
                    END) AS industry_occupation_type,

            MAX(NULL::text) AS creature_animal_cat,
            MAX(NULL::text) AS creature_animal_type,

            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'network and route building', 'crayon rail system')
                        THEN 'transportation'
                        END) AS transportation_travel_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'network and route building', 'crayon rail system')
                        THEN category_formatted
                    END) AS transportation_travel_type,

            MAX(
                CASE 
                    WHEN category_formatted IN ('race')
                        THEN 'sport'
                    END) AS sport_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN ('race')
                        THEN category_formatted
                    END) AS sport_type,

            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'team based', 'player elimination')
                        THEN 'player'
                    END) AS player_audience_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN ('team based', 'player elimination')
                        THEN category_formatted
                    END) AS player_audience_type      
        FROM MechRaw
        GROUP BY 1
    ),
    FamilyRaw AS (
        SELECT
            board_game_url,
            family AS category_raw,
            -- the prefix ('Theme: ', 'History: ') comes off before
            -- format_category(), because the macro removes ':'
            {{ format_category("REGEXP_REPLACE(family, '(^.*:)', '')") }} AS category_formatted,
            CONCAT(LOWER(TRIM(REGEXP_REPLACE(family, '(:.*$)', '')))) AS category_type
        FROM {{ ref('stg_board_games') }}, UNNEST(family_list) AS family
        WHERE LOWER(TRIM(REGEXP_REPLACE(family, '(:.*$)', ''))) IN (
            'players', 'theme', 'components', 'series', 'crowdfunding', 'animals', 'category',
            'sports', 'digital implementations', 'history', 'misc', 'mechanism', 'creatures',
            'traditional games', 'tv shows', 'magazine', 'food & drink', 'card games', 'containers',
            'books', 'ancient', 'brands', 'states', 'movies', 'setting', 'religious', 'traditional card games',
            'collectible', 'holidays', 'comic books', 'contests', 'occupation', 'decades', 'organizations', 
            'mythology', 'space', 'folk tales & fairy tales', 'video game theme', 'war level of command',
            'trivia', 'toys', 'historical figures', 'sport teams', 'medical', 'constructions',
            'interwar period (nov. 1918 - aug. 1939)', 'word games', 'music', 'war battlespace', 'comic strips',
            'celebrities', 'rivers', 'mountains')
        GROUP BY 1, 2, 3
    ),
    Family AS (
        SELECT
            board_game_url,
            -- can be bool or high level cat
            MAX(
                CASE
                    WHEN category_formatted IN ('drone warfare', 'siege')
                        THEN 'war'
                    WHEN category_formatted IN (
                        'aboriginal australians', 'apache tribes', 'austronesian peoples', 'aztecs', 
                        'colonial', 'inuit peoples', 'māori', 'mayans', 'native americans or first peoples', 
                        '7 deadly sins', 'ninjas', 'chernobyl', 'chivalry or jousting or tournaments medieval europe',
                        'gladiators', 'samurai', 'vikings', 'hell', 'spooky old houses', 'temple')
                        THEN 'history'
                    WHEN category_type IN ('war level of command', 'war battlespace')
                        THEN 'war'
                    WHEN category_type IN ('history', 'ancient', 'religious', 'decades', 'historical figures', 
                        'interwar period (nov. 1918 - aug. 1939)')
                        THEN 'history'
                    END) AS history_war_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'drone warfare', 'siege', 
                        'aboriginal australians', 'apache tribes', 'austronesian peoples', 'aztecs', 
                        'colonial', 'inuit peoples', 'māori', 'mayans', 'native americans or first peoples',
                        '7 deadly sins', 'ninjas', 'chernobyl', 'chivalry or jousting or tournaments medieval europe', 
                        'gladiators', 'samurai', 'vikings', 'hell', 'spooky old houses', 'temple')
                        THEN category_formatted
                    WHEN category_type IN (
                        'war level of command', 'war battlespace', 'history',
                        'ancient', 'religious', 'decades', 'historical figures',
                        'interwar period (nov. 1918 - aug. 1939)')
                        THEN category_formatted
                    END) AS history_war_type,
            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'robots', 'floating islands in the sky', 'ufos', 
                        'cryptids', 'cthulhu mythos', 'druids', 'kaiju', 'magical girls',
                        'superheroes', 'villainy', 'witches', 'wizards', 'fictional', 
                        'hanseatic league', 'king arthur or the knights of the round table or camelot',
                        'knights templar', 'medieval fantasy', 'romance of the three kingdoms',
                        'alternate history', 'cyberpunk', 'dieselpunk', 'dystopian age warcradle studios',
                        'fictional languages', 'mech warfare', 'post apocalyptic', 'solarpunk', 'steampunk', 'time travel', 
                        'astrology', 'psychic powers', 'dreams or nightmares')
                        THEN 'fantasy'
                    WHEN category_type IN (
                        'mythology', 'space', 'folk tales & fairy tales')
                        THEN 'fantasy'
                    END) AS fantasy_adventure_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'robots', 'floating islands in the sky', 'ufos', 
                        'cryptids', 'cthulhu mythos', 'druids', 'kaiju', 'magical girls',
                        'superheroes', 'villainy', 'witches', 'wizards', 'fictional',
                        'hanseatic league', 'king arthur or the knights of the round table or camelot',
                        'knights templar', 'medieval fantasy', 'romance of the three kingdoms',
                        'alternate history', 'cyberpunk', 'dieselpunk', 'dystopian age warcradle studios',
                        'fictional languages', 'mech warfare', 'post apocalyptic', 'solarpunk', 'steampunk', 'time travel',
                        'astrology', 'psychic powers', 'dreams or nightmares')
                        THEN category_formatted
                    WHEN category_type IN ('mythology', 'space', 'folk tales & fairy tales')
                        THEN category_formatted
                    END) AS fantasy_adventure_type,

            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'origami', 'rubik''s cube', 'sewing or knitting or cloth making', 'shelving', 'stained glass', 'stripping', 'alchemy')
                        THEN 'skill'
                    WHEN category_type IN ('contests', 'trivia', 'word games')
                        THEN 'skill'
                    END) AS skill_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'origami', 'rubik''s cube', 'sewing or knitting or cloth making', 'shelving', 'stained glass', 'stripping', 'alchemy')
                        THEN category_formatted
                    WHEN category_type IN ('contests', 'trivia', 'word games')
                        THEN category_formatted
                    END) AS skill_type,

            MAX(
                CASE 
                    WHEN category_formatted IN ('nuclear option')
                        THEN 'mechanism'
                    WHEN category_type IN ('mechanism')
                        THEN 'mechanism'
                    WHEN category_type IN ('series', 'category', 'digital implementations', 'misc') THEN 'attributes'
                    END) AS attribute_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN ('nuclear option')
                        THEN category_formatted
                    WHEN category_type IN ('mechanism', 'series',  'category', 'digital implementations', 'misc') 
                        THEN category_formatted
                    END) AS attribute_type,

            MAX(
                CASE 
                    WHEN category_formatted IN ('flags identification' )
                        THEN 'component'
                    WHEN category_type IN ('components', 'containers', 'collectible')
                        THEN 'component'
                    END) AS component_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN ('flags identification' )
                        THEN category_formatted
                    WHEN category_type IN ('components', 'containers', 'collectible')
                        THEN category_formatted
                    END) AS component_type,

            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'labour movement or workers rights or unionising', 'latin american political', 'revolution/uprising', 
                        'spanish political', 'climate change', 'environmental protection or degradation or pollution')
                        THEN 'politics'
                    WHEN category_formatted IN (
                        'plague epidemic', 'emergency management or disaster preparedness', 'mail or stamps or the post office', 
                        'firefighting')
                        THEN 'government'
                    WHEN category_formatted IN (
                        'archaeology or paleontology', 'genealogy or heredity', 'language or linguistics',
                        'mad science or mad scientist', 'school or college or university', 'teaching programming',
                        'astronomy', 'bacteria', 'biology', 'chemistry', 'evolution', 'physics', 'psychology','science')
                        THEN 'acedemia'
                    END) AS politics_gov_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'labour movement or workers rights or unionising', 'latin american political',  'revolution/uprising',
                        'spanish political', 'climate change', 'environmental protection or degradation or pollution', 
                        'plague epidemic', 'emergency management or disaster preparedness', 'mail or stamps or the post office', 
                        'firefighting', 
                        'archaeology or paleontology', 'genealogy or heredity', 'language or linguistics',
                        'mad science or mad scientist', 'school or college or university', 'teaching programming',
                        'astronomy', 'bacteria', 'biology', 'chemistry', 'evolution', 'physics', 'psychology','science')
                        THEN category_formatted
                    END) AS politics_gov_type,

            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'anime or manga', 'chibis', 'books or libraries')
                        THEN 'literary'
                    WHEN category_formatted IN (
                        'movie industry', 'movies', 'radio broadcasting', 'television tv industry', 'theater', 'video industry',
                        'arcade video', 'battle royale', 'boardgaming', 'boss battle', 'music making & makers')
                        THEN 'media'
                    WHEN category_formatted IN ('rodeos', 'circus', 'clowns', 'fireworks', 'kites', 'rainbows', 'dolls', 
                        'memes', 'excrements or feces or poop or urine', 'flatulence', 'trash or garbage', 'cemeteries or graveyards',
                        'african americans', 'birthdays', 'friendship', 'lgbtqia', 'love or romance', 'my best life',
                        'queer or lgbtaiq', 'advent or christmas or jule calendar', 'coziness or relaxation or self care or well being')
                        THEN 'culture'
                    WHEN category_formatted IN (
                        'amber', 'art', 'art style – art deco', 'art style – art nouveau', 'art style – tiki culture',
                        'fine art and art museums', 'geisha', 'graffiti', 'painting or paintings', 'photography', 'retro')
                        THEN 'art'
                    WHEN category_type IN ('magazine', 'books', 'comic books', 'comic strips')
                        THEN 'literary'
                    WHEN category_type IN ('tv shows', 'movies', 'video game theme', 'music')
                        THEN 'media'
                    WHEN category_type IN ('holidays', 'celebrities', 'toys')
                        THEN 'culture'
                    END) AS pop_culture_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'anime or manga', 'chibis', 'books or libraries', 
                        'movie industry', 'movies', 'radio broadcasting', 'television tv industry', 'theater', 'video industry',
                        'music making & makers', 
                        'arcade video', 'battle royale', 'boardgaming', 'boss battle',
                        'rodeos', 'circus', 'clowns', 'fireworks', 'kites', 'rainbows', 'dolls', 
                        'memes', 'excrements or feces or poop or urine', 'flatulence', 'trash or garbage', 
                        'cemeteries or graveyards', 'african americans', 'birthdays', 'friendship', 
                        'lgbtqia', 'love or romance', 'my best life',
                        'queer or lgbtaiq', 'advent or christmas or jule calendar', 'coziness or relaxation or self care or well being',
                        'amber', 'art', 'art style – art deco', 'art style – art nouveau', 'art style – tiki culture',
                        'fine art and art museums', 'geisha', 'graffiti', 'painting or paintings', 'photography', 'retro')
                        THEN category_formatted
                    WHEN category_type IN (
                        'magazine', 'books', 'comic books', 'comic strips', 'tv shows', 'movies','video game theme',
                        'music', 'holidays', 'celebrities', 'toys')
                        THEN category_formatted
                    END) AS pop_culture_type,

            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'sex workers', 'burglary and heists', 'hackers', 'infiltration', 'mystery or crime', 'pirates',
                        'police', 'smuggling', 'tv detectives', 'cannibals or cannibalism', 'harems')
                        THEN 'crime'
                    END) AS crime_violence_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'sex workers', 'burglary and heists', 'hackers', 'infiltration', 'mystery or crime', 'pirates',
                        'police', 'smuggling', 'tv detectives', 'cannibals or cannibalism', 'harems')
                        THEN category_formatted
                    END) AS crime_violence_type,
            
            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'cryptocurrency', 'silk road', 'stock market')
                        THEN 'economic'
                    WHEN category_formatted IN (
                        'journalism', 'automotive industry', 'computer or information technology industry',
                        'cereal', 'food or cooking', 'pulp', 'fashion', 'jewels', 
                        'perfume', 'factories', 'interiour design, home decoration, tidying', 'laundry & socks',
                        'business/management', 'construction', 'electricity generation', 'jewelry',
                        'metal smithing', 'mining', 'oil or gas or petroleum')
                        THEN 'industry'
                    WHEN category_formatted IN ('attorneys or courts')
                        THEN 'occupation'
                    WHEN category_type IN ('crowdfunding')
                        THEN 'economic'
                    WHEN category_type IN ('food & drink',  'medical', 'brands', 'organizations')
                        THEN 'industry'
                    WHEN category_type IN ('occupation') 
                        THEN 'occupation'
                    END) AS industry_occupation_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'cryptocurrency', 'silk road', 'stock market',
                        'journalism', 'automotive industry', 'computer or information technology industry',
                        'cereal', 'food or cooking', 'pulp', 'fashion', 'jewels', 
                        'perfume', 'factories', 'interiour design, home decoration, tidying', 'laundry & socks',
                        'business/management', 'construction', 'electricity generation', 'jewelry',
                        'metal smithing', 'mining', 'oil or gas or petroleum',
                        'attorneys or courts')
                        THEN category_formatted
                    WHEN category_type IN (
                        'crowdfunding', 'food & drink', 'medical', 'brands', 'organizations', 'occupation')
                        THEN category_formatted
                    END) AS industry_occupation_type,

            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'animal battles', 'animal migration', 'anthropomorphic animals', 'care bears',
                        'endangered or extinct species', 'hair or beards or animal fur', 'pets or companion animals', 
                        'toys or plushies or stuffed animals', 'winter rest or hibernation', 'anthropomorphic plants or food',
                        'ecology', 'sealife, seas, and oceans', 'seashells')
                        THEN 'animal'
                    WHEN category_formatted IN (
                        'mushrooms', 'flowers')
                        THEN 'flora'
                    WHEN category_type IN ('animals')
                        THEN 'animal'
                    WHEN category_type IN ('creatures')
                        THEN 'creature'
                    END) AS creature_animal_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'animal battles', 'animal migration', 'anthropomorphic animals', 'care bears',
                        'endangered or extinct species', 'hair or beards or animal fur', 'pets or companion animals', 
                        'toys or plushies or stuffed animals', 'winter rest or hibernation', 'anthropomorphic plants or food',
                        'ecology', 'sealife, seas, and oceans', 'seashells', 
                        'mushrooms', 'flowers')
                        THEN category_formatted
                    WHEN category_type IN ('animals', 'creatures')
                        THEN category_formatted
                    END) AS creature_animal_type,

            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'food trucks', 'helicopters', 'motorcycles', 'trucks', 'traffic or driving', 'cb radio', 'hot air balloons', 
                        'submarines', 'airships or blimps or dirigibles or zeppelins', 'cruise ships', 'orient express', 'trains')
                        THEN 'transportation'
                    WHEN category_formatted IN (
                        'earthquakes', 'volcanoes', 
                        'hotels, inns, lodgings', 'pub, bars, bistros...', 'restaurant/café', 'toilets or urinals', 
                        'camping', 'fishing or angling', 'french foreign legion', 'geocaching', 'hiking', 'rivers', 'scouting or guiding', 'survival',
                        'treasure diving', 'canals', 
                        'trees and forests', 'under the sea', 'wetlands or swamps or bogs or fens or marshland', 'beaches', 'cacti and succulents',
                        'deserts', 'disney theme parks', 'gardening', 'mountains', 'nature', 'safaris', 'seasons of the year', 'tropical', 'tropical islands',
                        'us national parks', 'weather', 'zoos, aquaria, safari parks', 'amusement parks or theme parks')
                        THEN 'travel'
                    WHEN category_type IN ('setting', 'rivers', 'mountains', 'constructions')
                        THEN 'travel'
                    END) AS transportation_travel_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN (
                        'food trucks', 'helicopters', 'motorcycles', 'trucks', 'traffic or driving', 'cb radio', 'hot air balloons', 'submarines', 
                        'airships or blimps or dirigibles or zeppelins', 'cruise ships', 'orient express', 'trains',
                        'earthquakes', 'volcanoes',
                        'hotels, inns, lodgings', 'pub, bars, bistros...', 'restaurant/café', 'toilets or urinals', 
                        'camping', 'fishing or angling', 'french foreign legion', 'geocaching', 'hiking', 'rivers', 'scouting or guiding', 'survival', 'treasure diving', 
                        'canals', 'trees and forests', 'under the sea', 'wetlands or swamps or bogs or fens or marshland', 'beaches', 'cacti and succulents',
                        'deserts', 'disney theme parks', 'gardening', 'mountains', 'nature', 'safaris', 'seasons of the year', 'tropical', 'tropical islands',
                        'us national parks', 'weather', 'zoos, aquaria, safari parks', 'amusement parks or theme parks')
                        THEN category_formatted
                    WHEN category_type IN ('setting', 'rivers', 'mountains', 'constructions')
                        THEN category_formatted
                    END) AS transportation_travel_type,

            MAX(
                CASE 
                    WHEN category_formatted IN ('fifa world cup', 'fantasy sports', 'sci fi sports')
                        THEN 'sport'
                    WHEN category_type IN ('sport teams', 'sports')
                        THEN 'sport'
                    END) AS sport_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN ('fifa world cup', 'fantasy sports', 'sci fi sports')
                        THEN category_formatted
                    WHEN category_type IN ('sport teams', 'sports')
                        THEN category_formatted
                    END) AS sport_type,

            MAX(
                CASE 
                    WHEN category_type IN ('players')
                        THEN 'player'
                    END) AS player_audience_cat,
            MAX(
                CASE 
                    WHEN category_type IN ('players')
                        THEN category_formatted
                    END) AS player_audience_type      
        FROM FamilyRaw
        GROUP BY 1
    ),
    MainCatRaw AS (
        SELECT
            board_game_url,
            main_category AS category_raw,
            {{ format_category('main_category') }} AS category_formatted
        FROM {{ ref('stg_board_games') }}, UNNEST(main_category_list) AS main_category
    ),
    BigMainCat AS (
        SELECT
            board_game_url,
            -- can be bool or high level cat
            MAX(
                CASE
                    WHEN category_formatted IN ('war')
                        THEN 'war'
                    END) AS history_war_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN ('war')
                        THEN category_formatted
                    END) AS history_war_type,
            MAX(NULL::text) AS fantasy_adventure_cat,
            MAX(NULL::text) AS fantasy_adventure_type,

            MAX(NULL::text) AS skill_cat,
            MAX(NULL::text) AS skill_type,

            MAX(
                CASE 
                    WHEN category_formatted IN ('strategy', 'abstract', 'thematic', 'customizable')
                        THEN 'attribute'
                    END) AS attribute_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN ('strategy', 'abstract', 'thematic', 'customizable')
                        THEN category_formatted
                    END) AS attribute_type,

            MAX(NULL::text) AS component_cat,
            MAX(NULL::text) AS component_type,

            MAX(NULL::text) AS politics_gov_cat,
            MAX(NULL::text) AS politics_gov_type,

            MAX(NULL::text) AS pop_culture_cat,
            MAX(NULL::text) AS pop_culture_type,

            MAX(NULL::text) AS crime_violence_cat,
            MAX(NULL::text) AS crime_violence_type,
            
            MAX(NULL::text) AS industry_occupation_cat,
            MAX(NULL::text) AS industry_occupation_type,

            MAX(NULL::text) AS creature_animal_cat,
            MAX(NULL::text) AS creature_animal_type,

            MAX(NULL::text) AS transportation_travel_cat,
            MAX(NULL::text) AS transportation_travel_type,

            MAX(NULL::text) AS sport_cat,
            MAX(NULL::text) AS sport_type,

            MAX(
                CASE 
                    WHEN category_formatted IN ('children''s', 'family', 'party')
                        THEN 'audience'
                    END) AS player_audience_cat,
            MAX(
                CASE 
                    WHEN category_formatted IN ('children''s', 'family', 'party')
                        THEN category_formatted
                    END) AS player_audience_type      
        FROM MainCatRaw
        GROUP BY 1
    )
    SELECT
        G.board_game_url,
        COALESCE(
            S.history_war_type,
            M.history_war_type,
            F.history_war_type,
            B.history_war_type
            ) AS history_war_type,
        COALESCE(
            S.history_war_cat,
            M.history_war_cat,
            F.history_war_cat,
            B.history_war_cat
            ) AS history_war_cat,
        CASE
            WHEN S.history_war_type IS NOT NULL THEN 'sub_cat'
            WHEN M.history_war_type IS NOT NULL THEN 'mech'
            WHEN F.history_war_type IS NOT NULL THEN 'family'
            WHEN B.history_war_type IS NOT NULL THEN 'main_cat'
            END AS history_war_source,
        
        COALESCE(
            S.fantasy_adventure_type,
            M.fantasy_adventure_type,
            F.fantasy_adventure_type,
            B.fantasy_adventure_type
            ) AS fantasy_adventure_type,
        COALESCE(
            S.fantasy_adventure_cat,
            M.fantasy_adventure_cat,
            F.fantasy_adventure_cat,
            B.fantasy_adventure_cat
            ) AS fantasy_adventure_cat,
        CASE
            WHEN S.fantasy_adventure_type IS NOT NULL THEN 'sub_cat'
            WHEN M.fantasy_adventure_type IS NOT NULL THEN 'mech'
            WHEN F.fantasy_adventure_type IS NOT NULL THEN 'family'
            WHEN B.fantasy_adventure_type IS NOT NULL THEN 'main_cat'
            END AS fantasy_adventure_source,
        
        COALESCE(
            S.skill_type,
            M.skill_type,
            F.skill_type,
            B.skill_type
            ) AS skill_type,
        COALESCE(
            S.skill_cat,
            M.skill_cat,
            F.skill_cat,
            B.skill_cat
            ) AS skill_cat,
        CASE
            WHEN S.skill_type IS NOT NULL THEN 'sub_cat'
            WHEN M.skill_type IS NOT NULL THEN 'mech'
            WHEN F.skill_type IS NOT NULL THEN 'family'
            WHEN B.skill_type IS NOT NULL THEN 'main_cat'
            END AS skill_source,
        
        COALESCE(
            S.attribute_type,
            M.attribute_type,
            F.attribute_type,
            B.attribute_type
            ) AS attribute_type,
        COALESCE(
            S.attribute_cat,
            M.attribute_cat,
            F.attribute_cat,
            B.attribute_cat
            ) AS attribute_cat,
        CASE
            WHEN S.attribute_type IS NOT NULL THEN 'sub_cat'
            WHEN M.attribute_type IS NOT NULL THEN 'mech'
            WHEN F.attribute_type IS NOT NULL THEN 'family'
            WHEN B.attribute_type IS NOT NULL THEN 'main_cat'
            END AS attribute_source,
        
        COALESCE(
            S.component_type,
            M.component_type,
            F.component_type,
            B.component_type
            ) AS component_type,
        COALESCE(
            S.component_cat,
            M.component_cat,
            F.component_cat,
            B.component_cat
            ) AS component_cat,
        CASE
            WHEN S.component_type IS NOT NULL THEN 'sub_cat'
            WHEN M.component_type IS NOT NULL THEN 'mech'
            WHEN F.component_type IS NOT NULL THEN 'family'
            WHEN B.component_type IS NOT NULL THEN 'main_cat'
            END AS component_source,
        
        COALESCE(
            S.politics_gov_type,
            M.politics_gov_type,
            F.politics_gov_type,
            B.politics_gov_type
            ) AS politics_gov_type,
        COALESCE(
            S.politics_gov_cat,
            M.politics_gov_cat,
            F.politics_gov_cat,
            B.politics_gov_cat
            ) AS politics_gov_cat,
        CASE
            WHEN S.politics_gov_type IS NOT NULL THEN 'sub_cat'
            WHEN M.politics_gov_type IS NOT NULL THEN 'mech'
            WHEN F.politics_gov_type IS NOT NULL THEN 'family'
            WHEN B.politics_gov_type IS NOT NULL THEN 'main_cat'
            END AS politics_gov_source,

        COALESCE(
            S.pop_culture_type,
            M.pop_culture_type,
            F.pop_culture_type,
            B.pop_culture_type
            ) AS pop_culture_type,
        COALESCE(
            S.pop_culture_cat,
            M.pop_culture_cat,
            F.pop_culture_cat,
            B.pop_culture_cat
            ) AS pop_culture_cat,
        CASE
            WHEN S.pop_culture_type IS NOT NULL THEN 'sub_cat'
            WHEN M.pop_culture_type IS NOT NULL THEN 'mech'
            WHEN F.pop_culture_type IS NOT NULL THEN 'family'
            WHEN B.pop_culture_type IS NOT NULL THEN 'main_cat'
            END AS pop_culture_source,

        COALESCE(
            S.crime_violence_type,
            M.crime_violence_type,
            F.crime_violence_type,
            B.crime_violence_type
            ) AS crime_violence_type,
        COALESCE(
            S.crime_violence_cat,
            M.crime_violence_cat,
            F.crime_violence_cat,
            B.crime_violence_cat
            ) AS crime_violence_cat,
        CASE
            WHEN S.crime_violence_type IS NOT NULL THEN 'sub_cat'
            WHEN M.crime_violence_type IS NOT NULL THEN 'mech'
            WHEN F.crime_violence_type IS NOT NULL THEN 'family'
            WHEN B.crime_violence_type IS NOT NULL THEN 'main_cat'
            END AS crime_violence_source,
        
        COALESCE(
            S.industry_occupation_type,
            M.industry_occupation_type,
            F.industry_occupation_type,
            B.industry_occupation_type
            ) AS industry_occupation_type,
        COALESCE(
            S.industry_occupation_cat,
            M.industry_occupation_cat,
            F.industry_occupation_cat,
            B.industry_occupation_cat
            ) AS industry_occupation_cat,
        CASE
            WHEN S.industry_occupation_type IS NOT NULL THEN 'sub_cat'
            WHEN M.industry_occupation_type IS NOT NULL THEN 'mech'
            WHEN F.industry_occupation_type IS NOT NULL THEN 'family'
            WHEN B.industry_occupation_type IS NOT NULL THEN 'main_cat'
            END AS industry_occupation_source,
        
        COALESCE(
            S.creature_animal_type,
            M.creature_animal_type,
            F.creature_animal_type,
            B.creature_animal_type
            ) AS creature_animal_type,
        COALESCE(
            S.creature_animal_cat,
            M.creature_animal_cat,
            F.creature_animal_cat,
            B.creature_animal_cat
            ) AS creature_animal_cat,
        CASE
            WHEN S.creature_animal_type IS NOT NULL THEN 'sub_cat'
            WHEN M.creature_animal_type IS NOT NULL THEN 'mech'
            WHEN F.creature_animal_type IS NOT NULL THEN 'family'
            WHEN B.creature_animal_type IS NOT NULL THEN 'main_cat'
            END AS creature_animal_source,
        
        COALESCE(
            S.transportation_travel_type,
            M.transportation_travel_type,
            F.transportation_travel_type,
            B.transportation_travel_type
            ) AS transportation_travel_type,
        COALESCE(
            S.transportation_travel_cat,
            M.transportation_travel_cat,
            F.transportation_travel_cat,
            B.transportation_travel_cat
            ) AS transportation_travel_cat,
        CASE
            WHEN S.transportation_travel_type IS NOT NULL THEN 'sub_cat'
            WHEN M.transportation_travel_type IS NOT NULL THEN 'mech'
            WHEN F.transportation_travel_type IS NOT NULL THEN 'family'
            WHEN B.transportation_travel_type IS NOT NULL THEN 'main_cat'
            END AS transportation_travel_source,

        COALESCE(
            S.sport_type,
            M.sport_type,
            F.sport_type,
            B.sport_type
            ) AS sport_type,
        COALESCE(
            S.sport_cat,
            M.sport_cat,
            F.sport_cat,
            B.sport_cat
            ) AS sport_cat,
        CASE
            WHEN S.sport_type IS NOT NULL THEN 'sub_cat'
            WHEN M.sport_type IS NOT NULL THEN 'mech'
            WHEN F.sport_type IS NOT NULL THEN 'family'
            WHEN B.sport_type IS NOT NULL THEN 'main_cat'
            END AS sport_source,
        
        COALESCE(
            S.player_audience_type,
            M.player_audience_type,
            F.player_audience_type,
            B.player_audience_type
            ) AS player_audience_type,
        COALESCE(
            S.player_audience_cat,
            M.player_audience_cat,
            F.player_audience_cat,
            B.player_audience_cat
            ) AS player_audience_cat,
        CASE
            WHEN S.player_audience_type IS NOT NULL THEN 'sub_cat'
            WHEN M.player_audience_type IS NOT NULL THEN 'mech'
            WHEN F.player_audience_type IS NOT NULL THEN 'family'
            WHEN B.player_audience_type IS NOT NULL THEN 'main_cat'
            END AS player_audience_source
    FROM GameSpine AS G
    LEFT JOIN SubCat AS S USING (board_game_url)
    LEFT JOIN Mech AS M USING (board_game_url)
    LEFT JOIN Family AS F USING (board_game_url)
    LEFT JOIN BigMainCat AS B USING(board_game_url)