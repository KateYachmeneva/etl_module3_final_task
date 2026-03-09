import psycopg2

PG_HOST = "postgres"
PG_PORT = 5432
PG_DB = "airflow"
PG_USER = "airflow"
PG_PASSWORD = "airflow"


def get_pg():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


SQL = """
      drop table if exists etl.event_logs_part cascade;

      create table etl.event_logs_part (
                                           event_id varchar(50) not null,
                                           event_time timestamptz not null,
                                           event_type varchar(50) not null,
                                           page varchar(255),
                                           element varchar(100),
                                           primary key (event_id, event_time)
      ) partition by range (event_time);

      create table etl.event_logs_2024_01
          partition of etl.event_logs_part
          for values from ('2024-01-01') to ('2024-02-01');

      create table etl.event_logs_2024_02
          partition of etl.event_logs_part
          for values from ('2024-02-01') to ('2024-03-01');

      create table etl.event_logs_2024_03
          partition of etl.event_logs_part
          for values from ('2024-03-01') to ('2024-04-01');

      insert into etl.event_logs_part (event_id, event_time, event_type, page, element)
      select event_id, event_time, event_type, page, element
      from etl.event_logs; \
      """


def main():
    conn = get_pg()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(SQL)
        print("Partitioned event_logs created successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()