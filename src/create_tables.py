from common import create_connection, DEFAULT_DB_NAME
from sql_queries import DROP_TABLE_QUERIES, CREATE_TABLE_QUERIES


def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """

    # connect to default database
    conn = create_connection()
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    db_name = DEFAULT_DB_NAME
    print("Database name", db_name)
    cur.execute(f"DROP DATABASE IF EXISTS {db_name}")
    cur.execute(f"CREATE DATABASE {db_name} WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()

    # connect to sparkify database
    conn = create_connection(db_name)
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `DROP_TABLE_QUERIES` list.
    """
    for query in DROP_TABLE_QUERIES:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `CREATE_TABLE_QUERIES` list.
    """
    for query in CREATE_TABLE_QUERIES:
        cur.execute(query)
        conn.commit()

def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  

    - Creates all tables sequences.

    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
