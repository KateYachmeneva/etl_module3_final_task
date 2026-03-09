import sys
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


def build_user_activity_datamart():
    sql = """
          drop table if exists etl.dm_user_activity;

          create table etl.dm_user_activity as
          with sessions as (
              select
                  user_id,
                  session_id,
                  session_duration_sec
              from etl.user_sessions
          ),
               pages as (
                   select
                       us.user_id,
                       count(sp.page_url) as pages_visited_count
                   from etl.user_sessions us
                            left join etl.session_pages sp
                                      on us.session_id = sp.session_id
                   group by us.user_id
               ),
               actions as (
                   select
                       us.user_id,
                       count(sa.action_name) as actions_count
                   from etl.user_sessions us
                            left join etl.session_actions sa
                                      on us.session_id = sa.session_id
                   group by us.user_id
               )
          select
              s.user_id,
              count(distinct s.session_id) as sessions_count,
              round(avg(s.session_duration_sec), 2) as avg_session_duration_sec,
              coalesce(p.pages_visited_count, 0) as pages_visited_count,
              coalesce(a.actions_count, 0) as actions_count
          from sessions s
                   left join pages p on s.user_id = p.user_id
                   left join actions a on s.user_id = a.user_id
          group by s.user_id, p.pages_visited_count, a.actions_count;

          create index if not exists idx_dm_user_activity_user_id
              on etl.dm_user_activity(user_id); \
          """

    conn = get_pg()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        print("Datamart etl.dm_user_activity created successfully.")
    finally:
        conn.close()


def build_support_efficiency_datamart():
    sql = """
          drop table if exists etl.dm_support_efficiency;

          create table etl.dm_support_efficiency as
          select
              issue_type,
              status,
              count(*) as tickets_count,
              round(avg(resolution_time_hours), 2) as avg_resolution_time_hours
          from etl.support_tickets
          group by issue_type, status
          order by issue_type, status;

          create index if not exists idx_dm_support_eff_issue_type
              on etl.dm_support_efficiency(issue_type);

          create index if not exists idx_dm_support_eff_status
              on etl.dm_support_efficiency(status); \
          """

    conn = get_pg()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        print("Datamart etl.dm_support_efficiency created successfully.")
    finally:
        conn.close()
def build_popular_pages_datamart():
    sql = """
          drop table if exists etl.dm_popular_pages;

          create table etl.dm_popular_pages as
          select
              page_url,
              count(*) as visits_count
          from etl.session_pages
          group by page_url
          order by visits_count desc;

          create index if not exists idx_dm_popular_pages_page_url
              on etl.dm_popular_pages(page_url); \
          """

    conn = get_pg()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        print("Datamart etl.dm_popular_pages created successfully.")
    finally:
        conn.close()

if __name__ == "__main__":
    command = sys.argv[1]

    if command == "build_user_activity":
        build_user_activity_datamart()
    elif command == "build_support_efficiency":
        build_support_efficiency_datamart()
    elif command == "build_popular_pages":
        build_popular_pages_datamart()
    else:
        raise ValueError(f"Unknown command: {command}")