import psycopg2

import create_tables
import etl
from src.sql_queries import TableNames


class TestETL:
    def test_etl(self):
        create_tables.main()
        etl.main()

        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()
        cur.execute(
            f"SELECT COUNT(*) FROM {TableNames.SONGPLAYS} WHERE song_id IS NOT NULL AND "
            f"artist_id IS NOT NULL ")
        count = cur.fetchone()[0]
        assert count == 1, f"""Songplays table should contain 1 record with artist_id " \
                            and song_id set but was {count}"""
        for table_name in TableNames.ALL_TABLES:
            cur.execute(f"SELECT COUNT(*) FROM {table_name} ")
            count = cur.fetchone()[0]
            assert count > 1, f"Table {table_name} should contain records while actual " \
                              f"count {count}"
