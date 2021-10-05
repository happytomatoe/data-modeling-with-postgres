import pytest

import create_tables
import etl
import table_names
from common import create_connection, DEFAULT_DB_NAME


class TestETL:

    @pytest.fixture(autouse=True)
    def run_before_and_after_tests(self):
        create_tables.create_database()
        create_tables.main()
        yield

    def test_songplays_table_contains_one_record_with_artist_and_song_id(self):
        etl.main()
        conn = create_connection(DEFAULT_DB_NAME)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {table_names.SONGPLAYS} WHERE song_id IS NOT NULL AND "
                f"artist_id IS NOT NULL ")
            count = cur.fetchone()
            print(count)
            assert count[0] == 1

    def test_that_each_table_has_records(self):
        etl.main()
        conn = create_connection(DEFAULT_DB_NAME)
        with conn.cursor() as cur:
            for table_name in table_names.ALL_TABLES:
                cur.execute(f"SELECT COUNT(*) FROM {table_name} ")
                count = cur.fetchone()[0]
                assert count > 1, f"Table {table_name} should contain records while actual " \
                                  f"count {count}"
