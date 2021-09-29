import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events (
        id INT IDENTITY(0,1),
        artist VARCHAR(255),
        auth VARCHAR(255),
        firstName VARCHAR(50),
        gender VARCHAR(20),
        itemInSession INT,
        lastName VARCHAR(50),
        length DOUBLE PRECISION,
        level VARCHAR(20),
        location VARCHAR(255),
        method VARCHAR(20),
        page VARCHAR(20),
        registration DOUBLE PRECISION,
        sessionId INT,
        song VARCHAR(255),
        status INT,
        ts VARCHAR(100),
        userAgent VARCHAR(255),
        userId INT
    );
""")

staging_songs_table_create = ("""
        CREATE TABLE staging_songs (
        id VARCHAR(50),
        num_songs INT, 
        artist_id VARCHAR(50), 
        artist_latitude DOUBLE PRECISION,
        artist_longitude DOUBLE PRECISION, 
        artist_location VARCHAR(255), 
        artist_name VARCHAR(255), 
        song_id VARCHAR(255),  
        title VARCHAR(255), 
        duration DOUBLE PRECISION, 
        year INT
    );
""")

songplay_table_create = ("""
        CREATE TABLE songplays (
        songplay_id INT IDENTITY(0,1), 
        start_time TIMESTAMP, 
        user_id INT, 
        level VARCHAR(20), 
        song_id VARCHAR(255), 
        artist_id VARCHAR(50), 
        session_id INT, 
        location VARCHAR(255), 
        user_agent VARCHAR(255),
        PRIMARY KEY (songplay_id)
        )
""")

user_table_create = ("""
        CREATE TABLE users (
        user_id INT NOT NULL, 
        first_name VARCHAR(50), 
        last_name VARCHAR(50), 
        gender VARCHAR(20), 
        level VARCHAR(20),
        PRIMARY KEY (user_id)
        )
""")

song_table_create = ("""
        CREATE TABLE songs (
        song_id VARCHAR(255) NOT NULL, 
        title VARCHAR(255), 
        artist_id VARCHAR(50), 
        year INT, 
        duration DOUBLE PRECISION,
        PRIMARY KEY (song_id)
        )
""")

artist_table_create = ("""
        CREATE TABLE artists (
        artist_id VARCHAR(50) NOT NULL, 
        name VARCHAR(255), 
        location VARCHAR(255), 
        lattitude DOUBLE PRECISION, 
        longitude DOUBLE PRECISION,
        PRIMARY KEY (artist_id)
        )
""")

time_table_create = ("""
        CREATE TABLE time (
        start_time TIMESTAMP NOT NULL, 
        hour INT, 
        day INT, 
        week INT, 
        month INT, 
        year INT, 
        weekday INT,
        PRIMARY KEY (start_time)
        )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
        iam_role {}
        json {};
""").format(config.get('S3','LOG_DATA'), 
            config.get('IAM_ROLE', 'ARN'), 
            config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs from {}
        iam_role {}
        json '{}';
""").format(config.get('S3','SONG_DATA'), 
            config.get('IAM_ROLE', 'ARN'),
           'auto')

# FINAL TABLES

songplay_table_insert = ("""
    insert into songplays (start_time, 
        user_id, 
        level, 
        song_id, 
        artist_id, 
        session_id, 
        location, 
        user_agent)
    select timestamp 'epoch' +  evt.ts/1000 *INTERVAL '1 second' AS start_time, evt.userId, evt.level, sng.song_id, sng.artist_id, evt.sessionId, evt.location, evt.userAgent
    from staging_songs sng, staging_events evt
    where evt.page = 'NextSong' and sng.title = evt.song and sng.artist_name = evt.artist and sng.duration = evt.length
""")


user_table_insert = ("""
    insert into users (user_id, 
        first_name, 
        last_name, 
        gender, 
        level)
    select DISTINCT evt.userId, evt.firstName, evt.lastName, evt.gender, evt.level
    from staging_events evt
    where evt.page = 'NextSong'
""")


song_table_insert = ("""
    insert into songs (song_id, 
        title, 
        artist_id, 
        year, 
        duration)
    select DISTINCT sng.song_id, sng.title, sng.artist_id, sng.year, sng.duration
    from staging_songs sng
    where sng.title is not null
""")


artist_table_insert = ("""
    insert into artists (artist_id, 
        name, 
        location, 
        lattitude, 
        longitude)
    select DISTINCT sng.artist_id, sng.artist_name, sng.artist_location, sng.artist_latitude, sng.artist_longitude
    from staging_songs sng
    where sng.artist_name is not null
""")

time_table_insert = ("""
    insert into time (start_time, 
        hour, 
        day, 
        week, 
        month, 
        year, 
        weekday)
    select DISTINCT sp.start_time, 
    EXTRACT(hour from sp.start_time), 
    EXTRACT(day from sp.start_time), 
    EXTRACT(week from sp.start_time),
    EXTRACT(month from sp.start_time),
    EXTRACT(year from sp.start_time),
    EXTRACT(weekday from sp.start_time)
    from songplays sp
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
