import create_tables
import etl
import table_names
from common import create_connection, DEFAULT_DB_NAME


class TestETL:

    def test_etl(self):
        create_tables.main()
        etl.main()
        with create_connection(DEFAULT_DB_NAME) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*) FROM {table_names.SONGPLAYS} WHERE song_id IS NOT NULL AND "
                    f"artist_id IS NOT NULL ")
                count = cur.fetchone()[0]
                assert count == 1, f"""Songplays table should contain 1 record with artist_id " \
                                    and song_id set but was {count}"""
                for table_name in table_names.ALL_TABLES:
                    cur.execute(f"SELECT COUNT(*) FROM {table_name} ")
                    count = cur.fetchone()[0]
                    assert count > 1, f"Table {table_name} should contain records while actual " \
                                      f"count {count}"
