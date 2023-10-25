import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

###############################---DROP-TABLES---##################################
# If the tables already exist, create_tables.py can be run whenever needed to reset the database and test ETL pipeline

staging_events_table_drop = "DROP TABLE IF EXISTS stage_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"



###############################---CREATE-TABLES---##################################

#__________________________1. Create Staging tables___________________________________#

#1.1 stage_events table - this is where Log Dataset will be landing
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS stage_events
    (
        artist        TEXT,
        auth          TEXT,
        firstName     TEXT,
        gender        TEXT,
        iteninSession INTEGER,
        lastName      TEXT,
        length        FLOAT4,
        level         TEXT,
        location      TEXT,
        method        TEXT,
        page          TEXT,
        registration  FLOAT8,
        sessionId     INTEGER,
        song          TEXT,
        status        INTEGER,
        ts            BIGINT,
        userAgent     TEXT,
        user_id       TEXT
    );
""")

#1.2 stage_songs table - this is where Song Dataset will be landing
staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS stage_songs
    (
        num_songs         INTEGER, 
        artist_id         TEXT, 
        artist_latitude   FLOAT4,
        artist_longitude  FLOAT4,
        artist_location   TEXT,
        artist_name       TEXT,
        song_id           TEXT,
        title             TEXT,
        duration          FLOAT4,
        year              INTEGER
    );
""")


#_________________________2. Create Final tables (Star Shcema)___________________________________#

#2.1 songplay (fact table) - records in event data associated with song plays i.e. records with page [NextSong]
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay
    (
        songplay_id    INTEGER IDENTITY(1,1) PRIMARY KEY,
        start_time     TIMESTAMP NOT NULL SORTKEY,
        user_id        TEXT NOT NULL, 
        level          TEXT, 
        song_id        TEXT NOT NULL, 
        artist_id      TEXT NOT NULL, 
        session_id     INTEGER, 
        location       TEXT, 
        user_agent     TEXT
    );
""")

#2.2 users (dimension table) - users in the app
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        user_id     TEXT PRIMARY KEY, 
        first_name  TEXT, 
        last_name   TEXT, 
        gender      TEXT, 
        level       TEXT
    );
""")

#2.3 song (dimensiona table) - songs in music database
song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song
    (
        song_id     TEXT PRIMARY KEY,
        title       TEXT, 
        artist_id   TEXT NOT NULL, 
        year        INTEGER, 
        duration    FLOAT4
    );
""")

#2.4 artist - artists in music database
artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist
    (
        artist_id   TEXT PRIMARY KEY, 
        name        TEXT, 
        location    TEXT, 
        latitude    FLOAT4, 
        longitude   FLOAT4
    ); 
""")

#2.5 time (dimension table) - timestamps of records in songplays broken down into specific units
time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        start_time TIMESTAMP PRIMARY KEY, 
        hour       INTEGER, 
        day        INTEGER, 
        week       INTEGER,
        month      INTEGER, 
        year       INTEGER, 
        weekday    INTEGER
    );
""")

###############################---END OF CREATE-TABLES---##################################




###############################---COPY TO STAGING TABLES---##################################
#--temporary tables used to hold, cleansed and standardized data before loading to the final tables)--#

#1. copy data from JSON file path to stage_events table
staging_events_copy = ("""
    copy {}
    from {}
    iam_role {}
    json {};                        /* Load from JSON data using a JSONPaths - use to map the source data to the table columns */
""").format('stage_events',
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'], 
    config['S3']['LOG_JSONPATH']   
)    

#2. copy data from JSON data to stage_songs table
staging_songs_copy = ("""
    copy {}
    from {}
    iam_role {}
    json 'auto';                   /* Load from JSON data using the 'auto' option (the key names must match the column names) */
""").format('stage_songs',         
    config['S3']['SONG_DATA'],     
    config['IAM_ROLE']['ARN']    
)

###############################---ENF OF COPY TO STAGING TABLES---##################################






###############################---INSERT TO FINAL TABLES---##################################
#----Use INSERT ** INTO ** SELECT ** Syntax to copy columns from staging table into final table----#

#1. insert columns from stage_events & stage_songs into songplay table
songplay_table_insert = ("""
    INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT TIMESTAMP 'epoch' + (e.ts/1000 * interval '1 second'),
           e.user_id,
           e.level,
           s.song_id,
           s.artist_id,
           e.sessionId,
           e.location,
           e.userAgent
    FROM stage_events e
    LEFT JOIN stage_songs s 
        ON e.artist = s.artist_name AND
           e.song   = s.title
    WHERE e.page = 'NextSong' AND
          e.user_id IS NOT NULL AND
          s.song_id IS NOT NULL AND
          s.artist_id IS NOT NULL;cn
""")

#2. insert columns from stage_events into users table
user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT user_id, 
           firstName, 
           lastName, 
           gender, 
           level
    FROM stage_events;
""")

#3. insert columns from stage_songs into song table
song_table_insert = ("""
    INSERT INTO song (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id,
           title, 
           artist_id, 
           year, 
           duration  
    FROM stage_songs;
""")

#4. insert columns from stage_songs into artist table
artist_table_insert = ("""
    INSERT INTO artist (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id,
           artist_name,
           artist_location,
           artist_latitude, 
           artist_longitude
    FROM stage_songs;
""")

#5. extract timestamp 'ts' column from stage_events and insert into time table
#THIS QUERY --> select timestamp 'epoch' + ts/1000 * interval '1 second' <--  is used to convert epoch to timestamp in Redshift
#ts/1000 converts milliseconds to seconds
time_table_insert = ("""
    INSERT INTO time
        WITH temp_time
        AS (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' as ts FROM stage_events)
    SELECT DISTINCT ts,
           extract(hour from ts),
           extract(day from ts),
           extract(week from ts),
           extract(month from ts),
           extract(year from ts),
           extract(weekday from ts)
    FROM temp_time
""")

###############################---END OF INSERT TO FINAL TABLES---##################################




# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
