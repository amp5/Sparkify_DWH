import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = " DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS song_play;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events (
                                artist varchar,
                                auth varchar,
                                first_name varchar,
                                gender varchar,
                                item_in_session integer,
                                last_name varchar,
                                length numeric, 
                                level varchar,
                                location varchar, 
                                method varchar,
                                page varchar,
                                registration numeric,
                                session_id integer,
                                song varchar, 
                                status int, 
                                ts bigint,
                                user_agent varchar,
                                user_id integer
                                    )
""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs (
                                    num_songs integer,
                                    artist_id varchar,
                                    artist_latitude numeric,
                                    artist_longitude numeric,
                                    artist_location varchar,
                                    artist_name varchar,
                                    song_id varchar,
                                    title varchar,
                                    duration numeric,
                                    year integer)
""")


songplay_table_create = (""" CREATE TABLE IF NOT EXISTS song_play (
                                songplay_id int IDENTITY(0,1) PRIMARY KEY,
                                start_time timestamp NOT NULL, 
                                user_id integer NOT NULL, 
                                song_id varchar, 
                                artist_id varchar, 
                                level text,
                                session_id integer,
                                location varchar
                                )
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users (
                            user_id integer NOT NULL PRIMARY KEY, 
                            first_name varchar,
                            last_name varchar,
                            gender varchar,
                            level varchar
                            )
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs(
                            song_id text NOT NULL PRIMARY KEY,
                            title varchar NOT NULL,
                            artist_id text NOT NULL, 
                            year int, 
                            duration numeric
                            )
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists(
                                artist_id text NOT NULL PRIMARY KEY, 
                                name varchar,
                                location varchar,
                                latitude numeric,
                                longitude numeric
                                )
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time(
                            start_time timestamp NOT NULL PRIMARY KEY,
                            hour int, 
                            day int, 
                            week int, 
                            month varchar,
                            year int,
                            weekday varchar
    )
""")

# STAGING TABLES

staging_events_copy = ("""
                        copy staging_events
                        from {}
                        iam_role {}
                        compupdate off region 'us-west-2'
                        json {}
                        EMPTYASNULL
                        BLANKSASNULL
                    """).format(config['S3']['LOG_DATA'], 
                                config['IAM_ROLE']['ARN'], 
                                config['S3']['LOG_JSONPATH'])


staging_songs_copy = ("""
                        copy staging_songs
                        from {}
                        iam_role {}
                        compupdate off region 'us-west-2'
                        json 'auto'
                    """).format(config['S3']['SONG_DATA'], 
                                config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO song_play(
                                start_time, 
                                user_id, 
                                song_id, 
                                artist_id, 
                                level,
                                session_id,
                                location 
                                )
                                SELECT 
                                    TIMESTAMP 'epoch' +(se.ts/1000)*interval'1 second'as start_time,
                                    se.user_id,
                                    ss.song_id,
                                    ss.artist_id,
                                    se.level,
                                    se.session_id,
                                    se.location
                                FROM staging_events se
                                JOIN staging_songs as ss
                                    ON (se.artist = ss.artist_name AND se.song = ss.title)
                                WHERE page='NextSong'
                                    AND se.length = ss.duration
""")

user_table_insert = (""" INSERT INTO users(
                            user_id, 
                            first_name,
                            last_name,
                            gender,
                            level 
                            )
                            SELECT
                                distinct user_id,
                                first_name,
                                last_name,
                                gender,
                                level
                            FROM staging_events
                            where page='NextSong'
""")

song_table_insert = (""" INSERT INTO songs (
                            song_id, 
                            title, 
                            artist_id, 
                            year, 
                            duration)
                            SELECT 
                                distinct song_id,
                                title,
                                artist_id,
                                year,
                                duration
                            FROM staging_songs;
""")

artist_table_insert = (""" INSERT INTO  artists (
                            artist_id, 
                            name, 
                            location,
                            latitude,
                            longitude)
                            SELECT 
                                artist_id,
                                artist_name,
                                artist_location,
                                artist_latitude,
                                artist_longitude
                            FROM staging_songs;                          
""")

time_table_insert = ("""  INSERT INTO time (
                            start_time, 
                            hour, 
                            day, 
                            week, 
                            month, 
                            year, 
                            weekday)
                          SELECT
                                t_start_time as start_time
                                , EXTRACT(HOUR FROM t_start_time) as hour
                                , EXTRACT(DAY FROM t_start_time) as day
                                , EXTRACT(WEEK FROM t_start_time) as week
                                , EXTRACT(MONTH FROM t_start_time) as month
                                , EXTRACT(YEAR FROM t_start_time) as year
                                , EXTRACT(DOW FROM t_start_time) as weekday
                            FROM
                                (SELECT distinct TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1second' as t_start_time FROM staging_events)
                            WHERE t_start_time is not NULL
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]


