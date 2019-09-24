import configparser
import psycopg2
from sql_queries import drop_table_queries, create_table_queries, copy_table_queries, insert_table_queries, \
    data_quality_check_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def data_quality_check(cur, conn):
    for query in data_quality_check_queries:
        cur.execute(query)
        (records,) = cur.fetchone()
        if records < 1:
            print(f"FAILED! Query: '{query}' returned {records} records.")
        else:
            print(f"SUCCESS! Query: '{query}' returned {records} records")
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    host = config.get("CLUSTER", "HOST")
    dbname = config.get("CLUSTER", "DB_NAME")
    user = config.get("CLUSTER", "DB_USER")
    password = config.get("CLUSTER", "DB_PASSWORD")
    port = config.get("CLUSTER", "DB_PORT")

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(host, dbname, user, password, port))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    data_quality_check(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
