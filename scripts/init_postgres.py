import psycopg2

PG_HOST = "postgres"
PG_PORT = 5432
PG_DB = "airflow"
PG_USER = "airflow"
PG_PASSWORD = "airflow"


DDL = """
create schema if not exists etl;

create table if not exists etl.user_sessions (
    session_id varchar(50) primary key,
    user_id varchar(50) not null,
    start_time timestamptz not null,
    end_time timestamptz not null,
    session_duration_sec integer not null,
    device_type varchar(20),
    device_os varchar(30),
    browser varchar(30)
);

create table if not exists etl.session_pages (
    session_id varchar(50) not null,
    page_order integer not null,
    page_url varchar(255) not null,
    primary key (session_id, page_order),
    constraint fk_session_pages_session
        foreign key (session_id) references etl.user_sessions(session_id)
        on delete cascade
);

create table if not exists etl.session_actions (
    session_id varchar(50) not null,
    action_order integer not null,
    action_name varchar(100) not null,
    primary key (session_id, action_order),
    constraint fk_session_actions_session
        foreign key (session_id) references etl.user_sessions(session_id)
        on delete cascade
);

create table if not exists etl.event_logs (
    event_id varchar(50) primary key,
    event_time timestamptz not null,
    event_type varchar(50) not null,
    page varchar(255),
    element varchar(100)
);

create table if not exists etl.support_tickets (
    ticket_id varchar(50) primary key,
    user_id varchar(50) not null,
    status varchar(30) not null,
    issue_type varchar(50) not null,
    created_at timestamptz not null,
    updated_at timestamptz not null,
    resolution_time_hours numeric(10,2) not null
);

create table if not exists etl.ticket_messages (
    ticket_id varchar(50) not null,
    message_order integer not null,
    sender varchar(20) not null,
    message_text text not null,
    message_time timestamptz not null,
    primary key (ticket_id, message_order),
    constraint fk_ticket_messages_ticket
        foreign key (ticket_id) references etl.support_tickets(ticket_id)
        on delete cascade
);

create index if not exists idx_user_sessions_user_id
    on etl.user_sessions(user_id);

create index if not exists idx_event_logs_event_time
    on etl.event_logs(event_time);

create index if not exists idx_event_logs_event_type
    on etl.event_logs(event_type);

create index if not exists idx_support_tickets_user_id
    on etl.support_tickets(user_id);

create index if not exists idx_support_tickets_status
    on etl.support_tickets(status);

create index if not exists idx_support_tickets_issue_type
    on etl.support_tickets(issue_type);
"""


def main():
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(DDL)
        print("PostgreSQL schema and tables created successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()