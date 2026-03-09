# etl_module3_final_task
# ETL Pipeline: MongoDB → PostgreSQL with Apache Airflow

## Описание проекта

В рамках итогового задания по дисциплине **ETL-процессы** был реализован ETL-pipeline для обработки пользовательских данных.

Pipeline выполняет следующие этапы:

1. Генерация тестовых данных в MongoDB
2. Репликация данных из MongoDB в PostgreSQL
3. Трансформация данных в реляционную структуру
4. Построение аналитических витрин
5. Проверка качества данных
6. Дополнительная оптимизация хранения данных с использованием партиционирования

Оркестрация всех процессов выполняется с использованием **Apache Airflow**.

Все скриншоты работы системы представлены в отдельном **PDF-файле отчёта**.

---

## Архитектура решения

Pipeline состоит из нескольких этапов:

MongoDB
↑
generate_mongo_data DAG
↓
mongo_to_postgres_etl DAG
↓
PostgreSQL (schema etl)
↓
data_quality_checks
↓
build_datamarts DAG
↓
Analytical Data Marts

Используемые технологии:

* Apache Airflow
* PostgreSQL
* MongoDB
* Docker / Docker Compose
* Python
* psycopg2
* pymongo

---

## Структура проекта

project

├─ airflow
│  ├─ dags
│  │  ├─ generate_mongo_data_dag.py
│  │  ├─ mongo_to_postgres_dag.py
│  │  ├─ datamarts_dag.py
│  │  └─ partition_event_logs_dag.py
│
├─ scripts
│  ├─ generate_mongo_data.py
│  ├─ load_user_sessions.py
│  ├─ init_postgress.py
│  ├─ build_datamarts.py
│  ├─ data_quality_checks.py
│  └─ create_partitioned_event_logs.py
│
├─ docker-compose.yml
│
└─ report.pdf (скриншоты и описание выполнения)

---

## Генерация данных

Данные генерируются в **MongoDB** в трёх коллекциях:

* user_sessions
* event_logs
* support_tickets

Каждая коллекция содержит **1000 записей**, сгенерированных случайным образом.

Генерация выполняется DAG:

generate_mongo_data

Task:

generate_data_in_mongo

---

## Репликация MongoDB → PostgreSQL

ETL-pipeline выполняет перенос данных из MongoDB в PostgreSQL.

DAG:

mongo_to_postgres_etl

Основные этапы pipeline:

create_schema
↓
load_user_sessions
↓
load_event_logs
↓
load_support_tickets
↓
run_data_quality_checks

Во время трансформации:

* массив pages_visited преобразуется в таблицу session_pages
* массив actions → session_actions
* массив messages → ticket_messages

В PostgreSQL создаётся схема:

etl

Таблицы:

* user_sessions
* session_pages
* session_actions
* event_logs
* support_tickets
* ticket_messages

---

## Проверка качества данных (Data Quality)

После загрузки данных выполняется автоматическая проверка качества.

DAG task:

run_data_quality_checks

Проверяется:

* отсутствие дублей по ключам
* наличие данных в таблицах
* корректность загрузки ETL

Если проверка не проходит — задача завершается с ошибкой.

---

## Аналитические витрины

Построение витрин выполняется DAG:

build_datamarts

Pipeline:

build_user_activity_datamart
↓
build_support_efficiency_datamart
↓
build_popular_pages_datamart

---

## Витрина 1 — Активность пользователей

Таблица:

etl.dm_user_activity

Содержит:

* user_id
* sessions_count
* avg_session_duration_sec
* pages_visited_count
* actions_count

Используется для анализа поведения пользователей.

---

## Витрина 2 — Эффективность работы поддержки

Таблица:

etl.dm_support_efficiency

Поля:

* issue_type
* status
* tickets_count
* avg_resolution_time_hours

Позволяет анализировать работу службы поддержки.

---

## Дополнительная витрина

Для расширения аналитических возможностей создана дополнительная витрина:

etl.dm_popular_pages

Она показывает наиболее посещаемые страницы:

* page_url
* visits_count

Это позволяет анализировать популярность контента.

---

## Партиционирование event_logs

Для оптимизации хранения событийных данных реализовано **партиционирование таблицы событий**.

DAG:

partition_event_logs

Создаётся таблица:

etl.event_logs_part

с диапазонным партиционированием по времени события.

Партиции:

* event_logs_2024_01
* event_logs_2024_02
* event_logs_2024_03

Это повышает производительность аналитических запросов.

---

## Как запустить проект

### 1. Запуск инфраструктуры

docker compose up -d

### 2. Airflow UI

http://localhost:8080

### 3. MongoDB UI

http://localhost:8082

---

## Скриншоты выполнения

Все скриншоты работы системы представлены в **PDF-отчёте**, включая:

* DAG генерации данных
* DAG ETL-репликации
* DAG построения витрин
* DAG партиционирования
* структуры таблиц PostgreSQL
* примеры данных витрин

---

## Результат

В рамках проекта реализован полноценный **ETL-pipeline**, включающий:

* генерацию данных
* репликацию MongoDB → PostgreSQL
* трансформацию данных
* построение аналитических витрин
* контроль качества данных
* оптимизацию хранения через партиционирование

Проект демонстрирует основные подходы **Data Engineering и разработки ETL-процессов**.
