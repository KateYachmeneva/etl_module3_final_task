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


def check_query(cur, query: str, error_message: str):
    cur.execute(query)
    result = cur.fetchone()[0]
    if not result:
        raise ValueError(error_message)


def main():
    conn = get_pg()
    try:
        with conn.cursor() as cur:
            check_query(
                cur,
                """
                select count(*) = count(distinct session_id)
                from etl.user_sessions
                """,
                "Duplicate session_id found in etl.user_sessions",
            )

            check_query(
                cur,
                """
                select count(*) = count(distinct event_id)
                from etl.event_logs
                """,
                "Duplicate event_id found in etl.event_logs",
            )

            check_query(
                cur,
                """
                select count(*) = count(distinct ticket_id)
                from etl.support_tickets
                """,
                "Duplicate ticket_id found in etl.support_tickets",
            )

            check_query(
                cur,
                """
                select count(*) > 0 from etl.user_sessions
                """,
                "etl.user_sessions is empty",
            )

            check_query(
                cur,
                """
                select count(*) > 0 from etl.event_logs
                """,
                "etl.event_logs is empty",
            )

            check_query(
                cur,
                """
                select count(*) > 0 from etl.support_tickets
                """,
                "etl.support_tickets is empty",
            )

        print("Data quality checks passed successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()