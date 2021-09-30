import glob
import os
import sys
from io import StringIO
from uuid import uuid4

import numpy as np
import pandas as pd
import psycopg2

import table_names
from common import create_connection, DEFAULT_DB_NAME
from sql_queries import SONG_SELECT

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

    # insert artist record
    artist_df = df[["artist_id", "artist_name", "artist_location", "artist_latitude",
                    "artist_longitude"]]
    load_into_db(cur, artist_df, table_names.ARTISTS)

    # insert song record
    song_df = df[['song_id', "title", "artist_id", "year", "duration"]]
    load_into_db(cur, song_df, table_names.SONGS)


def process_log_file(cur, filepath):
    """
    Processes log file
    :param cur - db cursor
    :param filepath - path to json file with logs
    """
    # open log file
    log_df = pd.read_json(path_or_buf=filepath, lines=True)

    # filter by NextSong action
    log_df = log_df[log_df.page == "NextSong"]

    # remove quotes in User Agent field
    log_df['userAgent'] = log_df['userAgent'].str.replace('"', '')

    # convert timestamp column to datetime
    log_df['start_time'] = pd.to_datetime(log_df["ts"], unit='ms')

    # create and load time data
    time_data_df = log_df[['start_time']].copy()
    datetime = time_data_df.start_time.dt
    time_data_df['hour'] = datetime.hour
    time_data_df['day'] = datetime.day
    time_data_df['week_of_year'] = datetime.isocalendar().week
    time_data_df['month'] = datetime.month
    time_data_df['year'] = datetime.year
    time_data_df['weekday'] = datetime.weekday
    time_data_df = time_data_df.drop_duplicates(subset=['start_time'])
    load_into_db(cur, time_data_df, table_names.TIME)

    # load user table
    user_df = log_df[["userId", "firstName", "lastName", "gender", "level"]].copy()
    user_df = user_df.drop_duplicates(subset=['userId'])
    load_into_db(cur, user_df, table_names.USERS)

    # populate and load songplays
    common_columns = ['song', 'artist', 'length']
    tuples = [tuple(x) for x in log_df[common_columns].to_numpy()]
    df2 = select_song_and_artist_ids(cur, tuples)
    if not df2.empty:
        log_df = pd.merge(log_df, df2, how='left', left_on=common_columns,
                          right_on=common_columns)
    else:
        log_df["artist_id"] = np.nan
        log_df["song_id"] = np.nan

    log_df['id'] = [str(uuid4()) for _ in range(len(log_df.index))]
    songplay_data = log_df[
        ['id', 'start_time', 'userId', 'level', 'song_id', 'artist_id',
         'sessionId', 'location', 'userAgent']]
    load_into_db(cur, songplay_data, table_names.SONGPLAYS)


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
        return pd.DataFrame(cur.fetchall(), columns=['song_id', 'artist_id', 'song', 'artist',
                                                     'length'])
    except psycopg2.Error as e:
        print("Exception while executing song select statement")
        print(e)
        sys.exit(1)


def load_into_db(cursor, dataframe, table_name):
    """
    Load pandas dataframe into table
    :param cursor: database cursor
    :param dataframe: pandas dataframe
    :param table_name: table where the data will be exported to
    """
    buffer = StringIO()
    dataframe.to_csv(buffer, header=False, index=False, sep="\t", na_rep='None')
    buffer.seek(0)

    try:
        cursor.execute(
            f'CREATE TEMP TABLE {TEMP_TABLE_NAME}(LIKE {table_name} INCLUDING ALL) ON COMMIT DROP; ')
        cursor.copy_from(buffer, TEMP_TABLE_NAME, sep="\t", null='None')
        cursor.execute(f"""
                INSERT INTO {table_name}
                SELECT * FROM {TEMP_TABLE_NAME}
                ON CONFLICT DO NOTHING;
                """)
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
    conn = create_connection(DEFAULT_DB_NAME)
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
