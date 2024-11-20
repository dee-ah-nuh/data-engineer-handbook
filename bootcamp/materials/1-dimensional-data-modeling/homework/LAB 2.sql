
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
--- DIMENSIONAL DATA MODELING 2
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------

-- WERE GONNA BE CHANGING TEH DATASETS INTO TYPE 2 SLOWLY CHANGING DIMENSIONS


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
    scorer_class SCORER_CLASS,
    years_since_last_season INTEGER,
    current_season INTEGER,
    is_active BOOLEAN,
    PRIMARY KEY(player_name, current_season)
);



INSERT INTO players
WITH years AS (
    SELECT *
    FROM GENERATE_SERIES(1996, 2022) AS season
), p AS (
    SELECT
        player_name,
        MIN(season) AS first_season
    FROM player_seasons
    GROUP BY player_name
), players_and_seasons AS (
    SELECT *
    FROM p
    JOIN years y
        ON p.first_season <= y.season
), windowed AS (
    SELECT
        pas.player_name,
        pas.season,
        ARRAY_REMOVE(
            ARRAY_AGG(
                CASE
                    WHEN ps.season IS NOT NULL
                        THEN ROW(
                            ps.season,
                            ps.gp,
                            ps.pts,
                            ps.reb,
                            ps.ast
                        )::season_stats
                END)
            OVER (PARTITION BY pas.player_name ORDER BY COALESCE(pas.season, ps.season)),
            NULL
        ) AS seasons
    FROM players_and_seasons pas
    LEFT JOIN player_seasons ps
        ON pas.player_name = ps.player_name
        AND pas.season = ps.season
    ORDER BY pas.player_name, pas.season
), static AS (
    SELECT
        player_name,
        MAX(height) AS height,
        MAX(college) AS college,
        MAX(country) AS country,
        MAX(draft_year) AS draft_year,
        MAX(draft_round) AS draft_round,
        MAX(draft_number) AS draft_number
    FROM player_seasons
    GROUP BY player_name
)
SELECT
    w.player_name,
    s.height,
    s.college,
    s.country,
    s.draft_year,
    s.draft_round,
    s.draft_number,
    seasons AS season_stats,
    CASE
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 20 THEN 'STAR'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 15 THEN 'GOOD'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 10 THEN 'AVG'
        ELSE 'BAD'
    END::scorer_class AS scorer_class,
    w.season - (seasons[CARDINALITY(seasons)]::season_stats).season as years_since_last_active,
    w.season,
    (seasons[CARDINALITY(seasons)]::season_stats).season = season AS is_active
FROM windowed w
JOIN static s
    ON w.player_name = s.player_name;

ALTER TABLE PLAYERS RENAME COLUMN SCORER_CLASS TO SCORING_CLASS;


--- CRETE A SERIES OF INDICATORS TO SEE IF THEIR STATUS REMAINED THE SAME OR NOT OR
-- WEATHER IT CHANGED
WITH with_previous AS (
    SELECT player_name,
           is_active,
           current_season,
           scoring_class,
           LAG(scoring_class, 1) over (partition by player_name order by current_season) as previous_scoring_class,
           LAG(is_active, 1) over (partition by player_name order by current_season) as previous_is_active
    FROM PLAYERS
)
SELECT *, CASE WHEN scoring_class <> previous_scoring_class THEN 1
            ELSE 0
            END AS SCORING_CLASS_CHANGE_INDICATOR,
        CASE WHEN is_active <> previous_is_active THEN 1
            ELSE 0
            END AS IS_ACTIVE_CHANGE_INDICATOR

FROM with_previous
;

INSERT INTO PLAYERS_SCD (
--- CREATE A DOUBLE-CTA FUNCTION THAT ALLOWS FOR MORE CLARITY
WITH with_previous AS (
    SELECT player_name,
           is_active,
           current_season,
           scoring_class,
           LAG(scoring_class, 1) over (partition by player_name order by current_season) as previous_scoring_class,
           LAG(is_active, 1) over (partition by player_name order by current_season) as previous_is_active
    FROM PLAYERS
    WHERE CURRENT_SEASON <= 2021
),

    WITH_INDICATORS AS (
                        SELECT *,   CASE WHEN scoring_class <> previous_scoring_class THEN 1
                                            WHEN is_active <> previous_is_active THEN 1
                                            ELSE 0
                                        END AS CHANGE_INDICATOR
                        FROM with_previous) ,
    WITH_STREAKS AS (
                    SELECT *,
                           SUM(CHANGE_INDICATOR) OVER (PARTITION BY player_name ORDER BY current_season) AS STREAK_IDENTIFIER
                    FROM WITH_INDICATORS )

SELECT PLAYER_NAME,
       STREAK_IDENTIFIER,
       IS_ACTIVE,
       SCORING_CLASS,
       MIN(CURRENT_SEASON) AS START_SEASON,
       MAX(CURRENT_SEASON) AS END_SEASON,
       2021 AS CURRENT_SEASON
FROM WITH_STREAKS
GROUP BY PLAYER_NAME, IS_ACTIVE, SCORING_CLASS, STREAK_IDENTIFIER
ORDER BY player_name, streak_identifier)

;


CREATE TYPE SCORING_CLASS AS ENUM ('STAR', 'GOOD', 'AVG', 'BAD');


drop table if exists PLAYERS_SCD;

CREATE TABLE PLAYERS_SCD (
    PLAYER_NAME TEXT,
    STREAK_IDENTIFIER INTEGER,
    IS_ACTIVE BOOLEAN,
    SCORING_CLASS scorer_class,
    start_season INTEGER,
    end_season INTEGER,
    current_season INTEGER,

    PRIMARY KEY(player_name, start_season)
);





SELECT * FROM PLAYERS_SCD;



WITH LAST_SEASON_SCD AS (

        SELECT * FROM PLAYERS_SCD
                 WHERE CURRENT_SEASON = 2021
                 AND END_SEASON = 2021
),

    HISTORICAL_SCD AS (
        SELECT PLAYER_NAME,
               SCORING_CLASS,
               IS_ACTIVE,
               start_season,
               end_season

        FROM PLAYERS_SCD
                 WHERE CURRENT_SEASON = 2021
                 AND END_SEASON < 2021 -- IF SOMEONE RETIRED AND CAME BACK THOSE RECORDS WILL NEVER CHANGE BECAUSE THEY STARTED AND ENDED.
), -- EG. MICHAEL JORDAN RETIRED IN 2000 BUT CAME BACK IN 2023, SO EVEN IF SOMETHING HAPPENED AFTER 2003 THOSE RECORDS WOULD NEVER CHANGE

     THIS_SEASON_DATA AS (

        SELECT * FROM PLAYERS
                 WHERE CURRENT_SEASON = 2022
    ),

    UNCHANGED_RECORDS AS (    SELECT TS.PLAYER_NAME,
                                 TS.SCORING_CLASS,
                                 TS.IS_ACTIVE AS TODAYS_SEASON_ACTIVITY,
                                 LS.START_SEASON,
                                 TS.CURRENT_SEASON AS END_SEASON

                          FROM THIS_SEASON_DATA TS
                          JOIN LAST_SEASON_SCD LS
                          ON LS.PLAYER_NAME = TS.player_name
                          WHERE TS.SCORING_CLASS = LS.SCORING_CLASS
                          AND TS.IS_ACTIVE = LS.IS_ACTIVE

    ),

    CHANGED_RECORDS AS (    SELECT TS.PLAYER_NAME,
                             UNNEST(ARRAY [
                                 ROW (
                                     LS.SCORING_CLASS,
                                     LS.IS_ACTIVE,
                                     LS.START_SEASON,
                                     LS.END_SEASON
                                     )::SCD_TYPE,
                                ROW (
                                     TS.SCORING_CLASS,
                                     TS.IS_ACTIVE,
                                     TS.CURRENT_SEASON,
                                     TS.CURRENT_SEASON
                                     )::SCD_TYPE
                                 ]) AS RECORDS




              FROM THIS_SEASON_DATA TS
              LEFT JOIN LAST_SEASON_SCD LS
              ON LS.PLAYER_NAME = TS.player_name
              WHERE (TS.SCORING_CLASS <> LS.SCORING_CLASS
              OR TS.IS_ACTIVE <> LS.IS_ACTIVE
              OR LS.PLAYER_NAME IS NULL)

),

    UNNESTED_CHANGED_RECORDS AS (

                SELECT PLAYER_NAME,
                       (RECORDS::SCD_TYPE).scoring_class,
                       (RECORDS::SCD_TYPE).IS_ACTIVE,
                       (RECORDS::SCD_TYPE).START_SEASON,
                       (RECORDS::SCD_TYPE).END_SEASON
                from CHANGED_RECORDS


    ),

    NEW_RECORDS AS(

        SELECT TS.PLAYER_NAME,
               TS.SCORING_CLASS,
               TS.IS_ACTIVE,
               TS.CURRENT_SEASON AS START_SEASON,
               TS.CURRENT_SEASON AS END_SEAON
        FROM THIS_SEASON_DATA TS
        LEFT JOIN LAST_SEASON_SCD LS
        ON TS.PLAYER_NAME = LS.PLAYER_NAME
        WHERE LS.PLAYER_NAME IS NULL

)

SELECT * from HISTORICAL_SCD
UNION ALL
SELECT * FROM UNCHANGED_RECORDS
UNION ALL
SELECT * FROM UNNESTED_CHANGED_RECORDS
UNION ALL
SELECT * FROM NEW_RECORDS


;

;

-- PROBLEM FOR THE RECORDS THAT DONT CHANGE WERE JUST GOING TO INCREASE 1,
-- BUT WHEN THEY DO CHANGE WERE GOING TO ADD ANOTHER RECORD (STRUCT)

    -- OPTION 1: UNCHANGED_RECORDS, WHERE WE SELECT FROM TODAY'S SEASON IF HES ACTIVE,
    -- THE CURRENT SCORING CLASS,
    -- LAST SEASONS SCORING CLASS, LOOK AT COLUMNS: TODAYS_SEASON_ACTIVITY & LAST_SEASON_ACTIVITY
    -- DID THEY CHANGE? NO.

    -- OPTION_2: IN UPDATED_RECORDS WE HAVE TO THINK ABOUT THE CHANGE
    -- IN STRUCT FORM TO INCLUDE THE OLD RECORD AND THE CHANGED RECORD
    -- CREATE A TYPE:

CREATE TYPE SCD_TYPE AS (

    SCORING_CLASS scorer_class,
    IS_ACTIVE BOOLEAN,
    START_SEASON INTEGER,
    END_SEASON INTEGER


    );

 -- finally we can see the tw records if in fact there has been a change from previous seasons:

-- Aaron Gordon,"(AVG,t,2020,2021)"
-- Aaron Gordon,"(GOOD,t,2022,2022)"
-- Aaron Henry,"(BAD,t,2020,2021)"
-- Aaron Henry,"(BAD,f,2022,2022)"
-- Aaron Nesmith,"(BAD,t,2020,2021)"
-- Aaron Nesmith,"(AVG,t,2022,2022)"



     --- option 3: flatten out of the struct
     -- OPTION 4: ADD THW NEW PLAYERS
     -- 39:35 / 45:38


     --FINAL TABLE:


     -- UNNESTED CHANGED YEARS
     -- COMPACTED DATA FROM 2021
     -- UNCHANGED RECORDS
     -- + NEW_RECORDS


-- Aaron Brooks,BAD,true,2007,2007
-- Aaron Brooks,AVG,true,2008,2008
-- Aaron Brooks,GOOD,true,2009,2009
-- Aaron Brooks,AVG,true,2010,2010
-- Aaron Brooks,AVG,false,2011,2011
-- Aaron Brooks,BAD,true,2012,2013
-- Aaron Brooks,AVG,true,2014,2015
-- Aaron Brooks,BAD,true,2016,2018



--- ASSUMPTIONS FOR THIS QUERY:
     -- ASSUME SCORING_CLASS AND IS_ACTIVE IS ALWAYS NOT NULL; IF NULL IT WILL BREAK THE PATTERN

     
     
     
     
     
     
     