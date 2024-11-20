-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
--- DIMENSIONAL DATA MODELING 1 
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------

select * from player_seasons;

CREATE TYPE season_stats AS (
        season INTEGER,
        gp INTEGER,
        pts REAL,
        reb REAL,
        ast REAL );


CREATE TYPE SCORER_CLASS AS ENUM ('STAR', 'GOOD', 'AVG', 'BAD');

drop table if exists players;

CREATE TABLE players (
    player_name TEXT,
    height TEXT,
    college TEXT,
    country TEXT,
    draft_year TEXT,
    draft_round TEXT,
    draft_number TEXT,
    season_stats season_stats[],
    scoring_class scoring_class,
    years_since_last_season INTEGER,
    current_season INTEGER,
    is_active BOOLEAN,
    PRIMARY KEY(player_name, current_season)
);


--- FULL OUTER JOIN LOGIC
INSERT INTO PLAYERS (
WITH YESTERDAY AS (

    SELECT * FROM PLAYERS WHERE CURRENT_SEASON = 2010
),

    TODAY AS (

        SELECT * FROM PLAYER_SEASONS WHERE SEASON = 2015
    )

SELECT

        -- COALESCE TO RETURN PLAYERS INFORMATION
        COALESCE(T.PLAYER_NAME, Y.PLAYER_NAME) AS PLAYER_NAME,
        COALESCE(T.HEIGHT, Y.HEIGHT) AS HEIGHT,
        COALESCE(T.college, Y.college) AS college,
        COALESCE(T.country, Y.country) AS country,
        COALESCE(T.draft_year, Y.draft_year) AS draft_year,
        COALESCE(T.draft_round, Y.draft_round) AS draft_round,
        COALESCE(T.draft_number, Y.draft_number) AS draft_number,


        -- IF YESTERDAYS SEASON STATS IS NULL THEN CONSTRUCT THE ARRAY
        -- IF TODAY'S SEASON IS NOT NULL THEN CONCAT YESTERDAYS SEASON STATS WITH TODAY'S SEASONS STATS
        -- ELSE JUST RETURN THE SEASON STATS FROM YESTERDAY FOR HISTORICAL DATA

        CASE WHEN Y.SEASON_STATS IS NULL
            THEN ARRAY[
                ROW(T.SEASON,
                    T.GP,
                    T.PTS,
                    T.REB,
                    T.ast):: season_stats]
        WHEN T.SEASON IS NOT NULL THEN Y.season_stats || ARRAY[
                ROW(T.SEASON,
                    T.GP,
                    T.PTS,
                    T.REB,
                    T.ast):: season_stats]
            ELSE Y.season_stats
        END AS SEASON_STATS,

        -- SCORING CLASS
        CASE
            WHEN T.SEASON IS NOT NULL THEN
            CASE WHEN T.PTS > 20 THEN 'STAR'
            WHEN T.PTS > 15 THEN 'GOOD'
            WHEN T.PTS > 10 THEN 'AVG'
            ELSE 'BAD'
            END::SCORING_CLASS
                ELSE Y.scoring_class
        END AS SCORING_CLASS,


        -- YEARS SINCE LAST SEASON PLAYED
        CASE
            WHEN T.SEASON IS NOT NULL THEN 0
            ELSE Y.years_since_last_season + 1
        END AS YEARS_SINCE_LAST_SEASON,


        --- WILL GIVE US CURRENT SEASON VALUE
        COALESCE(T.SEASON, Y.current_season + 1) AS CURRENT_SEASON,


         T.SEASON IS NOT NULL as is_active

FROM TODAY T FULL OUTER JOIN YESTERDAY Y
ON T.player_name = Y.player_name);


-- #####################################################
-- The COALESCE function:
-- Checks if the value from T (today's data) is null.
-- If it's not null, it uses T's value.
-- If it is null, it falls back to Y (yesterday's data).

-- #####################################################
-- The Array Construct:
-- WHEN Y.SEASON_STATS IS NULL:
-- Creates a new array containing a single ROW with today's season stats (T.SEASON, T.GP, T.PTS, T.REB, T.AST).
-- This initializes the stats if no historical data (Y.SEASON_STATS) exists.

-- WHEN T.SEASON IS NOT NULL:
-- Appends today's season stats (T.SEASON, T.GP, T.PTS, T.REB, T.AST) as a ROW to the existing Y.SEASON_STATS array.
-- This updates the stats by adding today's data to historical stats.

-- ELSE:
-- Returns Y.SEASON_STATS as is, ensuring continuity of historical data when no new stats (T.SEASON) are available.

-- #####################################################
-- The Season Coalesce
-- COALESCE(T.SEASON, Y.current_season + 1):
-- If T.SEASON (today's season) exists, it is used as the CURRENT_SEASON.
-- If T.SEASON is null, it defaults to Y.current_season + 1, which assumes
-- the next logical season based on the most recent historical season (Y.current_season).



WITH unnested AS (SELECT player_name,
                         unnest(season_stats):: season_stats AS season_stats

                  FROM PLAYERS
                  WHERE current_season = 2001
--                AND player_name = 'Michael Jordan'
)
SELECT
    player_name,
    (SEASON_STATS::SEASON_STATS).*

FROM unnested
;

-- RESULTS:
-- Art Long,    2000,9,0,0.9,0.1
-- Art Long,    2001,63,4.5,4,0.7

-- Aaron McKie, 1996,83,5.2,2.7,1.9
-- Aaron McKie, 1997,81,4.1,2.9,2.2
-- Aaron McKie, 1998,50,4.8,2.8,2
-- Aaron McKie, 1999,82,8,3,2.9
-- Aaron McKie, 2000,76,11.6,4.1,5
-- Aaron McKie, 2001,48,12.2,4,3.7





drop table players;


-------------- PROBLEM: WHICH PLAYER HAS IMPROVED THE GREATEST FROM THE FIRST SEASON TO THE CURRENT SEASON?

SELECT * FROM PLAYERS
WHERE current_season = 2001
AND player_name = 'Michael Jordan';


SELECT CURRENT_SEASON, COUNT(*) FROM PLAYERS GROUP BY CURRENT_SEASON;

-- HISTORICAL ANALYSIS NO NEED FOR GROUP BY OR SHUFFLE
SELECT
    player_name,
    (SEASON_STATS[CARDINALITY(season_stats)]::SEASON_STATS).pts /
    CASE WHEN(season_stats[1]::season_stats).pts = 0 THEN 1
        ELSE (season_stats[1]::season_stats).pts
    END as MOST_IMPROVED

FROM PLAYERS
WHERE current_season = 2001
AND scoring_class = 'STAR'
ORDER BY 2 DESC
;
