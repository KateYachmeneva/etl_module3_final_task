import random
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient

MONGO_URI = "mongodb://mongo:mongo@mongo:27017/"
DB_NAME = "etl_db"

SESSIONS_COUNT = 1000
EVENTS_COUNT = 1000
TICKETS_COUNT = 1000

PAGES = [
    "/home",
    "/catalog",
    "/products",
    "/products/101",
    "/products/205",
    "/products/333",
    "/cart",
    "/checkout",
    "/profile",
    "/support",
]

EVENT_TYPES = ["click", "view", "scroll", "search", "login", "logout", "purchase"]

ISSUE_TYPES = ["payment", "delivery", "account", "refund", "product_quality"]
TICKET_STATUSES = ["open", "in_progress", "closed"]

DEVICES = ["mobile", "desktop", "tablet"]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge"]
OPERATING_SYSTEMS = ["Windows", "macOS", "Linux", "Android", "iOS"]


def random_dt(start: datetime, end: datetime) -> datetime:
    delta = end - start
    seconds = int(delta.total_seconds())
    return start + timedelta(seconds=random.randint(0, seconds))


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def generate_user_sessions(count: int):
    sessions = []
    base_start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    base_end = datetime(2024, 3, 31, 23, 59, 59, tzinfo=timezone.utc)

    for i in range(1, count + 1):
        user_id = f"user_{random.randint(1, 200)}"
        start_time = random_dt(base_start, base_end)
        duration_minutes = random.randint(5, 120)
        end_time = start_time + timedelta(minutes=duration_minutes)

        pages_count = random.randint(2, 8)
        actions_count = random.randint(2, 8)

        pages_visited = random.choices(PAGES, k=pages_count)
        actions = random.choices(
            ["login", "view_page", "view_product", "search", "add_to_cart", "checkout", "logout"],
            k=actions_count,
        )

        session = {
            "session_id": f"sess_{i:05d}",
            "user_id": user_id,
            "start_time": iso_z(start_time),
            "end_time": iso_z(end_time),
            "pages_visited": pages_visited,
            "device": {
                "type": random.choice(DEVICES),
                "os": random.choice(OPERATING_SYSTEMS),
                "browser": random.choice(BROWSERS),
            },
            "actions": actions,
        }
        sessions.append(session)

    return sessions


def generate_event_logs(count: int):
    events = []
    base_start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    base_end = datetime(2024, 3, 31, 23, 59, 59, tzinfo=timezone.utc)

    for i in range(1, count + 1):
        page = random.choice(PAGES)
        event_type = random.choice(EVENT_TYPES)

        event = {
            "event_id": f"evt_{i:06d}",
            "timestamp": iso_z(random_dt(base_start, base_end)),
            "event_type": event_type,
            "details": {
                "page": page,
                "element": random.choice(
                    ["button", "link", "banner", "search_input", "menu_item", "product_card"]
                ),
            },
        }
        events.append(event)

    return events


def generate_support_tickets(count: int):
    tickets = []
    base_start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    base_end = datetime(2024, 3, 31, 23, 59, 59, tzinfo=timezone.utc)

    user_messages = [
        "Не могу оплатить заказ.",
        "Не пришло письмо с подтверждением.",
        "Не работает промокод.",
        "Хочу оформить возврат.",
        "Заказ задерживается.",
        "Проблема со входом в аккаунт.",
    ]

    support_messages = [
        "Пожалуйста, уточните номер заказа.",
        "Проверяем информацию, ожидайте.",
        "Проблема решена, попробуйте снова.",
        "Мы передали запрос в технический отдел.",
        "Возврат оформлен.",
    ]

    for i in range(1, count + 1):
        created_at = random_dt(base_start, base_end)
        status = random.choice(TICKET_STATUSES)

        if status == "open":
            updated_at = created_at + timedelta(hours=random.randint(1, 24))
        elif status == "in_progress":
            updated_at = created_at + timedelta(hours=random.randint(6, 72))
        else:
            updated_at = created_at + timedelta(hours=random.randint(12, 120))

        messages = [
            {
                "sender": "user",
                "message": random.choice(user_messages),
                "timestamp": iso_z(created_at + timedelta(minutes=5)),
            }
        ]

        extra_messages_count = random.randint(1, 3)
        current_time = created_at + timedelta(minutes=30)

        for _ in range(extra_messages_count):
            messages.append(
                {
                    "sender": random.choice(["support", "user"]),
                    "message": random.choice(
                        support_messages if random.random() > 0.5 else user_messages
                    ),
                    "timestamp": iso_z(current_time),
                }
            )
            current_time += timedelta(minutes=random.randint(20, 180))

        ticket = {
            "ticket_id": f"ticket_{i:05d}",
            "user_id": f"user_{random.randint(1, 200)}",
            "status": status,
            "issue_type": random.choice(ISSUE_TYPES),
            "messages": messages,
            "created_at": iso_z(created_at),
            "updated_at": iso_z(updated_at),
        }
        tickets.append(ticket)

    return tickets


def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    user_sessions_col = db["user_sessions"]
    event_logs_col = db["event_logs"]
    support_tickets_col = db["support_tickets"]

    # очищаем коллекции перед загрузкой
    user_sessions_col.delete_many({})
    event_logs_col.delete_many({})
    support_tickets_col.delete_many({})

    user_sessions = generate_user_sessions(SESSIONS_COUNT)
    event_logs = generate_event_logs(EVENTS_COUNT)
    support_tickets = generate_support_tickets(TICKETS_COUNT)

    user_sessions_col.insert_many(user_sessions)
    event_logs_col.insert_many(event_logs)
    support_tickets_col.insert_many(support_tickets)

    # индексы, чтобы выглядело аккуратно и профессионально
    user_sessions_col.create_index("session_id", unique=True)
    user_sessions_col.create_index("user_id")

    event_logs_col.create_index("event_id", unique=True)
    event_logs_col.create_index("timestamp")
    event_logs_col.create_index("event_type")

    support_tickets_col.create_index("ticket_id", unique=True)
    support_tickets_col.create_index("user_id")
    support_tickets_col.create_index("status")
    support_tickets_col.create_index("issue_type")

    print("Данные успешно загружены в MongoDB:")
    print(f"user_sessions: {user_sessions_col.count_documents({})}")
    print(f"event_logs: {event_logs_col.count_documents({})}")
    print(f"support_tickets: {support_tickets_col.count_documents({})}")

    client.close()


if __name__ == "__main__":
    main()