import os
from urllib.parse import urlparse

import psycopg2
from dotenv import load_dotenv

DEFAULT_DB_NAME = "sparkifydb"


def create_connection(db_name=None):
    """
    Loads env vars from .env file and
    creates connection to postgres db using POSTGRES_URL env var
    :param db_name - optional database name. Default value is parsed from the POSTGRES_URL
    :return:
    """
    load_dotenv()

    postgres_url = os.getenv('POSTGRES_URL')

    # Adapted from https://stackoverflow.com/questions/15634092/connect-to-an-uri-in-postgres
    result = urlparse(postgres_url)
    username = result.username
    password = result.password
    hostname = result.hostname
    port = result.port

    if db_name is None:
        db_name = result.path[1:]

    connection = psycopg2.connect(
        database=db_name,
        user=username,
        password=password,
        host=hostname,
        port=port
    )
    return connection
