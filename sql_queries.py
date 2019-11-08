"""
Fact Table
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables
users -
user_id, first_name, last_name, gender, level

songs -
song_id, title, artist_id, year, duration

artists -
artist_id, name, location, latitude, longitude

time -
start_time, hour, day, week, month, year, weekday
"""

# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = (""" 
    CREATE TABLE songplays 
    (
        songplay_id serial PRIMARY KEY,
        start_time numeric NOT NULL,
        user_id int NOT NULL,
        level varchar NOT NULL,
        song_id varchar,
        artist_id varchar,
        session_id int,
        location varchar,
        user_agent text
    );
""")

user_table_create = ("""
    CREATE TABLE users 
    (
        user_id int PRIMARY KEY,
        first_name varchar NOT NULL,
        last_name varchar,
        gender varchar(2) NOT NULL,
        level varchar NOT NULL
    );
""")

song_table_create = (""" 
    CREATE TABLE songs 
    (
        song_id varchar PRIMARY KEY,
        title varchar NOT NULL,
        artist_id varchar NOT NULL,
        year smallint,
        duration numeric NOT NULL
    );
""")

artist_table_create = (""" 
    CREATE TABLE artists 
    (
        artist_id varchar PRIMARY KEY,
        name text NOT NULL,
        location varchar,
        latitude float(6),
        longitude float(6)
    );
""")

time_table_create = (""" 
    CREATE TABLE time 
    (
        start_time numeric PRIMARY KEY,
        hour int NOT NULL,
        day int NOT NULL,
        week int NOT NULL,
        month int NOT NULL,
        year int NOT NULL,
        weekday int NOT NULL
    ); 
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays
    (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);
""")

user_table_insert = ("""
    INSERT INTO users
    (user_id, first_name, last_name, gender, level)
    VALUES (%s,%s,%s,%s,%s)
    ON CONFLICT(user_id) DO UPDATE SET
        first_name = COALESCE(EXCLUDED.first_name,users.first_name),
        last_name = COALESCE(EXCLUDED.last_name,users.last_name),
        gender = COALESCE(EXCLUDED.gender,users.gender),
        level = COALESCE(EXCLUDED.level,users.level);
""")

song_table_insert = ("""
    INSERT INTO songs
    (song_id, title, artist_id, year, duration)
    VALUES (%s,%s,%s,%s,%s)
    ON CONFLICT(song_id) DO UPDATE SET
        title = COALESCE(EXCLUDED.title,songs.title),
        artist_id = COALESCE(EXCLUDED.artist_id,songs.artist_id),
        year = COALESCE(EXCLUDED.year,songs.year),
        duration = COALESCE(EXCLUDED.duration,songs.duration);
""")

artist_table_insert = ("""
    INSERT INTO artists
    (artist_id, name, location, latitude, longitude)
    VALUES (%s,%s,%s,%s,%s)
    ON CONFLICT(artist_id) DO UPDATE SET
        name = COALESCE(EXCLUDED.name,artists.name),
        location = COALESCE(EXCLUDED.location,artists.location),
        latitude = COALESCE(EXCLUDED.latitude,artists.latitude),
        longitude = COALESCE(EXCLUDED.longitude,artists.longitude);
""")


time_table_insert = ("""
    INSERT INTO time
    (start_time, hour, day, week, month, year, weekday)
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT(start_time) DO NOTHING;
""")

## INSERT RECORDS FROM JSON FILE

user_table_insert_from_json=("""
    INSERT INTO users
    SELECT 
        DISTINCT cast(data->>'userId' AS integer) AS user_id, 
        data->>'firstName' AS first_name, 
        data->>'lastName' AS last_name, 
        data->>'gender' AS gender, 
        data->>'level' AS level
    FROM log_data_json
    WHERE data->>'userId' != ''
    AND data->>'level' = %s
    ON CONFLICT(user_id) DO UPDATE SET
        first_name = COALESCE(EXCLUDED.first_name,users.first_name),
        last_name = COALESCE(EXCLUDED.last_name,users.last_name),
        gender = COALESCE(EXCLUDED.gender,users.gender),
        level = COALESCE(EXCLUDED.level,users.level);
""")

songplay_table_insert_from_json=("""
    INSERT INTO songplays
    (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT 
        ld.start_time,
        ld.user_id,
        ld.level,
        s.song_id,
        a.artist_id,
        ld.session_id,
        ld.location,
        ld.user_agent
    FROM    
        (SELECT 
            cast(data->>'ts' AS numeric) AS start_time, 
            cast(data->>'userId' AS integer) AS user_id, 
            data->>'level' AS level, 
            data->>'song' AS song_title, 
            cast(data->>'length' AS numeric) AS length, 
            data->>'artist' AS artist_name, 
            cast(data->>'sessionId' AS integer) AS session_id, 
            data->>'location' AS location,
            data->>'userAgent' AS user_agent
        FROM log_data_json
        where data->>'page'='NextSong')ld
        LEFT OUTER JOIN
        (SELECT
            song_id,
            title,
            artist_id,
            duration
        FROM songs)s
        ON ld.song_title=s.title 
        AND ld.length=s.duration
        LEFT OUTER JOIN
        (SELECT
            artist_id,
            name
        FROM artists)a
        ON a.artist_id=s.artist_id
        AND a.name=ld.artist_name;
""")

# FIND SONGS

song_select = ("""
    SELECT * FROM
        (SELECT
            song_id,
            title,
            artist_id,
            duration
        FROM songs)s
        JOIN
        (SELECT
            artist_id,
            name
        FROM artists)a
        ON a.artist_id=s.artist_id
        WHERE s.title=%s
        AND a.name=%s
        AND s.duration=%s;
""")

# LOADING LOG FILE

drop_log_file_table="drop table if exists log_data_json;"
create_log_file_temp_table="CREATE temporary TABLE log_data_json (data jsonb NOT NULL);"
copy_data_from_json="copy log_data_json from %s;"

# TIMESTAMP FETCH
ts_fetch="""SELECT distinct cast(data->>'ts' AS numeric) AS start_time FROM log_data_json where data->>'page'='NextSong';"""

distinct_level_fetch="""SELECT distinct data->>'level' AS start_time FROM log_data_json ORDER BY data->>'level'"""

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]