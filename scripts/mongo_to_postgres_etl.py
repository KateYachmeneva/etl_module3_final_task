import sys
from datetime import datetime

import psycopg2
from pymongo import MongoClient

MONGO_URI = "mongodb://mongo:mongo@mongo:27017/"
MONGO_DB = "etl_db"

PG_HOST = "postgres"
PG_PORT = 5432
PG_DB = "airflow"
PG_USER = "airflow"
PG_PASSWORD = "airflow"


def parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def get_mongo():
    client = MongoClient(MONGO_URI)
    return client, client[MONGO_DB]


def get_pg():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def load_user_sessions():
    mongo_client, mongo_db = get_mongo()
    pg_conn = get_pg()

    try:
        docs = list(mongo_db["user_sessions"].find({}, {"_id": 0}))

        with pg_conn:
            with pg_conn.cursor() as cur:
                for doc in docs:
                    start_time = parse_dt(doc["start_time"])
                    end_time = parse_dt(doc["end_time"])
                    duration_sec = int((end_time - start_time).total_seconds())
                    device = doc.get("device", {})

                    cur.execute(
                        """
                        insert into etl.user_sessions (
                            session_id, user_id, start_time, end_time,
                            session_duration_sec, device_type, device_os, browser
                        )
                        values (%s, %s, %s, %s, %s, %s, %s, %s)
                            on conflict (session_id) do update set
                            user_id = excluded.user_id,
                                                            start_time = excluded.start_time,
                                                            end_time = excluded.end_time,
                                                            session_duration_sec = excluded.session_duration_sec,
                                                            device_type = excluded.device_type,
                                                            device_os = excluded.device_os,
                                                            browser = excluded.browser
                        """,
                        (
                            doc["session_id"],
                            doc["user_id"],
                            start_time,
                            end_time,
                            duration_sec,
                            device.get("type"),
                            device.get("os"),
                            device.get("browser"),
                        ),
                    )

                    cur.execute(
                        "delete from etl.session_pages where session_id = %s",
                        (doc["session_id"],),
                    )
                    for idx, page in enumerate(doc.get("pages_visited", []), start=1):
                        cur.execute(
                            """
                            insert into etl.session_pages (session_id, page_order, page_url)
                            values (%s, %s, %s)
                            """,
                            (doc["session_id"], idx, page),
                        )

                    cur.execute(
                        "delete from etl.session_actions where session_id = %s",
                        (doc["session_id"],),
                    )
                    for idx, action in enumerate(doc.get("actions", []), start=1):
                        cur.execute(
                            """
                            insert into etl.session_actions (session_id, action_order, action_name)
                            values (%s, %s, %s)
                            """,
                            (doc["session_id"], idx, action),
                        )

        print(f"Loaded user_sessions: {len(docs)}")
    finally:
        pg_conn.close()
        mongo_client.close()


def load_event_logs():
    mongo_client, mongo_db = get_mongo()
    pg_conn = get_pg()

    try:
        docs = list(mongo_db["event_logs"].find({}, {"_id": 0}))

        with pg_conn:
            with pg_conn.cursor() as cur:
                for doc in docs:
                    details = doc.get("details", {})

                    cur.execute(
                        """
                        insert into etl.event_logs (
                            event_id, event_time, event_type, page, element
                        )
                        values (%s, %s, %s, %s, %s)
                            on conflict (event_id) do update set
                            event_time = excluded.event_time,
                                                          event_type = excluded.event_type,
                                                          page = excluded.page,
                                                          element = excluded.element
                        """,
                        (
                            doc["event_id"],
                            parse_dt(doc["timestamp"]),
                            doc["event_type"],
                            details.get("page"),
                            details.get("element"),
                        ),
                    )

        print(f"Loaded event_logs: {len(docs)}")
    finally:
        pg_conn.close()
        mongo_client.close()


def load_support_tickets():
    mongo_client, mongo_db = get_mongo()
    pg_conn = get_pg()

    try:
        docs = list(mongo_db["support_tickets"].find({}, {"_id": 0}))

        with pg_conn:
            with pg_conn.cursor() as cur:
                for doc in docs:
                    created_at = parse_dt(doc["created_at"])
                    updated_at = parse_dt(doc["updated_at"])
                    resolution_time_hours = round(
                        (updated_at - created_at).total_seconds() / 3600, 2
                    )

                    cur.execute(
                        """
                        insert into etl.support_tickets (
                            ticket_id, user_id, status, issue_type,
                            created_at, updated_at, resolution_time_hours
                        )
                        values (%s, %s, %s, %s, %s, %s, %s)
                            on conflict (ticket_id) do update set
                            user_id = excluded.user_id,
                                                           status = excluded.status,
                                                           issue_type = excluded.issue_type,
                                                           created_at = excluded.created_at,
                                                           updated_at = excluded.updated_at,
                                                           resolution_time_hours = excluded.resolution_time_hours
                        """,
                        (
                            doc["ticket_id"],
                            doc["user_id"],
                            doc["status"],
                            doc["issue_type"],
                            created_at,
                            updated_at,
                            resolution_time_hours,
                        ),
                    )

                    cur.execute(
                        "delete from etl.ticket_messages where ticket_id = %s",
                        (doc["ticket_id"],),
                    )
                    for idx, msg in enumerate(doc.get("messages", []), start=1):
                        cur.execute(
                            """
                            insert into etl.ticket_messages (
                                ticket_id, message_order, sender, message_text, message_time
                            )
                            values (%s, %s, %s, %s, %s)
                            """,
                            (
                                doc["ticket_id"],
                                idx,
                                msg.get("sender"),
                                msg.get("message"),
                                parse_dt(msg["timestamp"]),
                            ),
                        )

        print(f"Loaded support_tickets: {len(docs)}")
    finally:
        pg_conn.close()
        mongo_client.close()


if __name__ == "__main__":
    command = sys.argv[1]

    if command == "load_user_sessions":
        load_user_sessions()
    elif command == "load_event_logs":
        load_event_logs()
    elif command == "load_support_tickets":
        load_support_tickets()
    else:
        raise ValueError(f"Unknown command: {command}")