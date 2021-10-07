class TableNames:
    TIME = "time"
    ARTISTS = "artists"
    SONGS = "songs"
    USERS = "users"
    SONGPLAYS = "songplays"
    ALL_TABLES = [TIME, ARTISTS, SONGPLAYS, SONGS, USERS]


DROP_TABLE_QUERY_TEMPLATE = "DROP TABLE IF EXISTS {};"

# DROP TABLES

SONGPLAY_TABLE_DROP = DROP_TABLE_QUERY_TEMPLATE.format(TableNames.SONGPLAYS)
USERS_TABLE_DROP = DROP_TABLE_QUERY_TEMPLATE.format(TableNames.USERS)
DROP_SONGS_TABLE = DROP_TABLE_QUERY_TEMPLATE.format(TableNames.SONGS)
ARTIST_TABLE_DROP = DROP_TABLE_QUERY_TEMPLATE.format(TableNames.ARTISTS)
TIME_TABLE_DROP = DROP_TABLE_QUERY_TEMPLATE.format(TableNames.TIME)

# CREATE TABLES

# alter table time owner to postgres;

SONGPLAY_TABLE_CREATE = (f"""
CREATE TABLE {TableNames.SONGPLAYS}(
    id uuid not null constraint songplays_pk primary key,
    start_time timestamp not null constraint songplays__time_fk references time,
    user_id bigint constraint songplays__user_fk references users,
    level varchar,
    song_id varchar constraint songplays__songs_fk references songs,
    artist_id varchar constraint songplays__artist_fk references artists,
    session_id bigint,
    location varchar,
    user_agent varchar
    );
""")

USER_TABLE_CREATE = (f"""
CREATE TABLE {TableNames.USERS} (
    user_id bigint not null constraint users_pk primary key,
    first_name varchar,
    last_name varchar,
    gender varchar(1),
    level varchar
    )
""")

SONG_TABLE_CREATE = (f"""
CREATE TABLE {TableNames.SONGS} (
    song_id varchar not null constraint songs_pk primary key,
    title varchar not null unique,
    artist_id varchar constraint songs__artist_fk references artists,
    year integer,
    duration numeric not null CHECK (duration >0)
    )
""")

ARTIST_TABLE_CREATE = (f"""
CREATE TABLE {TableNames.ARTISTS}(
    artist_id varchar not null constraint artists_pk primary key,
    name varchar not null unique,
    location varchar,
    latitude numeric CHECK (latitude >= -90 AND latitude <= 90),
    longitude numeric CHECK (latitude >= -180 AND latitude <= 180)
    )
""")

TIME_TABLE_CREATE = (f"""
CREATE TABLE {TableNames.TIME}(
    start_time timestamp not null constraint time_pk primary key,
    hour integer not null,
    day integer not null,
    week integer not null,
    month integer not null,
    year integer not null,
    weekday integer not null 
)
""")

# FIND SONGS
SONG_SELECT = (f"""
    SELECT s.song_id, s.artist_id,s.title,a.name,s.duration FROM {TableNames.SONGS} s
    JOIN {TableNames.ARTISTS} a ON s.artist_id=a.artist_id
    WHERE (s.title,a.name,s.duration) in ({{}}) 
    """)

# QUERY LISTS
CREATE_TABLE_QUERIES = [TIME_TABLE_CREATE, ARTIST_TABLE_CREATE, USER_TABLE_CREATE,
                        SONG_TABLE_CREATE, SONGPLAY_TABLE_CREATE]
DROP_TABLE_QUERIES = [SONGPLAY_TABLE_DROP, USERS_TABLE_DROP, DROP_SONGS_TABLE,
                      ARTIST_TABLE_DROP, TIME_TABLE_DROP]
