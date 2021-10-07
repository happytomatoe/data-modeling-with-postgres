import glob
import os
import sys
from io import StringIO
from uuid import uuid4

import numpy as np
import pandas as pd
import psycopg2
from numpy import float64

from sql_queries import SONG_SELECT, TableNames

TEMP_TABLE_NAME = 'source'


def process_song_file(cur, filepath):
    """
    Processes song file
    :param cur - db cursor
    :param filepath - filepath to json file with song data
    """
    # open song file
    df = pd.read_json(path_or_buf=filepath, lines=True)
    df = df.replace({'year': {0: np.nan}})

    # insert artist records
    artist_df = df[["artist_id", "artist_name", "artist_location", "artist_latitude",
                    "artist_longitude"]]
    load_into_db(cur, artist_df, TableNames.ARTISTS)

    # insert song records
    song_df = df[['song_id', "title", "artist_id", "year", "duration"]]
    load_into_db(cur, song_df, TableNames.SONGS)


def process_log_file(cur, filepath):
    """
    Processes log file
    :param cur - db cursor
    :param filepath - path to json file with logs
    """
    # open log file
    log_df = pd.read_json(path_or_buf=filepath, lines=True)

    log_df = log_df[log_df.page == "NextSong"]

    # remove quotes in User Agent field
    log_df['userAgent'] = log_df['userAgent'].str.replace('"', '')

    # convert timestamp column to datetime
    log_df['start_time'] = pd.to_datetime(log_df["ts"], unit='ms')

    process_time_data(cur, log_df)

    process_users(cur, log_df)

    process_songplays(cur, log_df)


def process_songplays(cur, log_df):
    """
    Create/populate songplays dataframe and load it into db
    :param cur:  db cursor
    :param log_df: dataframe with log data
    """
    common_columns = ['song', 'artist', 'length']
    tuples = [tuple(x) for x in log_df[common_columns].values]
    df2 = select_song_and_artist_ids(cur, tuples)
    if not df2.empty:
        log_df = log_df.merge(df2, how='left', on=common_columns)
    else:
        log_df["artist_id"] = np.nan
        log_df["song_id"] = np.nan
    log_df['id'] = [str(uuid4()) for _ in range(len(log_df.index))]
    songplay_data = log_df[
        ['id', 'start_time', 'userId', 'level', 'song_id', 'artist_id',
         'sessionId', 'location', 'userAgent']]
    load_into_db(cur, songplay_data, TableNames.SONGPLAYS)


def process_users(cur, log_df):
    """
    Create/populate time data dataframe and load it into db
    :param cur:  db cursor
    :param log_df: dataframe with log data
    """
    user_df = log_df[["userId", "firstName", "lastName", "gender", "level"]].copy()
    user_df = user_df.drop_duplicates(subset=['userId'])
    load_into_db(cur, user_df, TableNames.USERS, "user_id", ["level"])


def process_time_data(cur, log_df):
    """
    Create/populate time data dataframe and load it into db
    :param cur:  db cursor
    :param log_df: dataframe with log data
    """
    time_data_df = log_df[['start_time']].copy()
    datetime = time_data_df.start_time.dt
    time_data_df['hour'] = datetime.hour
    time_data_df['day'] = datetime.day
    time_data_df['week_of_year'] = datetime.week
    time_data_df['month'] = datetime.month
    time_data_df['year'] = datetime.year
    time_data_df['weekday'] = datetime.weekday
    time_data_df = time_data_df.drop_duplicates(subset=['start_time'])
    load_into_db(cur, time_data_df, TableNames.TIME)


def select_song_and_artist_ids(cur, tuples):
    """
    Selects an artist_id and song_id from db using song, artist, song duration
    :param cur: db cursor
    :param tuples: tuples in which every element contains song, artist, song duration(length)
    :return: pandas dataframe with 'song_id', 'artist_id', 'song', 'artist',
            'length' columns
    """
    if len(tuples) == 0 or len(tuples[0]) == 0:
        return pd.DataFrame()

    placeholder = ','.join("%s" for _ in range(0, len(tuples[0])))
    args_str = ','.join(cur.mogrify(f"({placeholder})", x).decode('utf-8') for x in tuples)
    try:
        cur.execute(SONG_SELECT.format(args_str))
        data_frame = pd.DataFrame(cur.fetchall(),
                                  columns=['song_id', 'artist_id', 'song', 'artist', 'length'])
        # convert column length to same type as in log df
        data_frame['length'] = data_frame['length'].astype(float64)
        return data_frame
    except psycopg2.Error as e:
        print("Exception while executing song select statement")
        print(e)
        sys.exit(1)


def load_into_db(cursor, dataframe, table_name, primary_key_column="", update_columns=None):
    """
    Load pandas dataframe into table
    :param cursor: database cursor
    :param dataframe: pandas dataframe
    :param table_name: table where the data will be exported to
    :param primary_key_column: which is used as contraint in ON CONFLICT ... UPDATE statement
    :param update_columns: columns set in CONFLICT ... UPDATE statement when inserting data

    """
    # Adapted from https://medium.com/analytics-vidhya/part-4-pandas-dataframe-to-postgresql
    # -using-python-8ffdb0323c09
    if update_columns is None:
        update_columns = []
    buffer = StringIO()
    dataframe.to_csv(buffer, header=False, index=False, sep="\t", na_rep='None')
    buffer.seek(0)

    try:
        # Adapted from https://stackoverflow.com/questions/48019381/how-postgresql-copy-to-stdin
        # -with-csv-do -on-conflic-do-update
        cursor.execute(
            f'''CREATE TEMP TABLE {TEMP_TABLE_NAME}(LIKE {table_name} INCLUDING ALL) 
                ON COMMIT DROP;''')
        cursor.copy_from(buffer, TEMP_TABLE_NAME, sep="\t", null='None')

        if update_columns is not None and len(primary_key_column) > 0:
            on_conflict_action = f"UPDATE SET {update_columns[0]}=excluded" \
                               f".{update_columns[0]}"
            primary_key_column = f"({primary_key_column})"
        else:
            on_conflict_action = "NOTHING"
        insert_query = f"""
                INSERT INTO {table_name}
                SELECT * FROM {TEMP_TABLE_NAME}
                ON CONFLICT {primary_key_column} DO {on_conflict_action}
                """
        print(insert_query)
        cursor.execute(insert_query)
        cursor.execute(f'DROP TABLE {TEMP_TABLE_NAME}')
    except psycopg2.Error as e:
        print("Exception while loading data into db")
        print(e)
        sys.exit(1)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, _, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
