from prefect import flow, task
import requests
import os

CH_URL = os.getenv("CH_URL", "http://localhost:8123")

CH_USER = os.getenv("CLICKHOUSE_USER", "etl")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "etl_password")


# ============================================================
# TASK 1 — подготовка таблиц
# ============================================================

@task(name="Create ClickHouse Aggregation Tables")
def create_clickhouse_tables():
    tables = {
        "taxi_by_hour": """
            CREATE TABLE IF NOT EXISTS taxi_by_hour (
                hour UInt8,
                trips UInt32,
                avg_fare Float32,
                avg_km Float32,
                avg_passengers Float32
            ) ENGINE = ReplacingMergeTree()
            ORDER BY hour
        """,
        "taxi_by_dow": """
            CREATE TABLE IF NOT EXISTS taxi_by_dow (
                dow UInt8,
                is_weekend UInt8,
                trips UInt32,
                avg_fare Float32,
                avg_km Float32,
                avg_passengers Float32
            ) ENGINE = ReplacingMergeTree()
            ORDER BY dow
        """,
        "taxi_by_month": """
            CREATE TABLE IF NOT EXISTS taxi_by_month (
                ym String,
                trips UInt32,
                total_revenue Float64,
                avg_fare Float32,
                avg_km Float32
            ) ENGINE = ReplacingMergeTree()
            ORDER BY ym
        """
    }

    for name, sql in tables.items():
        requests.post(
            CH_URL,
            params={"query": sql.strip()},
            auth=(CH_USER, CH_PASSWORD)
        ).raise_for_status()

        print(f"ClickHouse: {name} готова")


# ============================================================
# TASK 2 — загрузка raw данных в ClickHouse
# ============================================================

@task(name="Load Raw Data from Postgres to ClickHouse")
def load_raw_data_into_clickhouse():
    requests.post(
        CH_URL,
        params={"query": "DROP TABLE IF EXISTS taxi_raw_ch"},
        auth=(CH_USER, CH_PASSWORD)
    ).raise_for_status()

    def sql_literal(value):
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"

    create_sql = f"""
        CREATE TABLE taxi_raw_ch ENGINE = MergeTree()
        ORDER BY tuple() AS
        SELECT *
        FROM postgresql(
            {sql_literal(os.getenv("POSTGRES_HOST", "postgres:5432"))},
            {sql_literal(os.getenv("POSTGRES_DB", "etl"))},
            'taxi_raw',
            {sql_literal(os.getenv("POSTGRES_USER", "postgres"))},
            {sql_literal(os.getenv("POSTGRES_PASSWORD", "postgres"))}
        )
    """

    requests.post(
        CH_URL,
        params={"query": create_sql},
        auth=(CH_USER, CH_PASSWORD)
    ).raise_for_status()

    print("ClickHouse: raw данные загружены из Postgres")


# ============================================================
# TASK 3 — агрегация по часам
# ============================================================

@task(name="Aggregate Trips by Hour (ClickHouse)")
def aggregate_by_hour():
    requests.post(
        CH_URL,
        params={"query": "TRUNCATE TABLE taxi_by_hour"},
        auth=(CH_USER, CH_PASSWORD)
    ).raise_for_status()

    query = """
        INSERT INTO taxi_by_hour
        SELECT
            toHour(pickup_datetime) AS hour,
            COUNT() AS trips,
            round(avg(fare_amount), 2) AS avg_fare,
            round(
                avg(
                    greatCircleDistance(
                        pickup_longitude,
                        pickup_latitude,
                        dropoff_longitude,
                        dropoff_latitude
                    ) / 1000
                ),
                2
            ) AS avg_km,
            round(avg(passenger_count), 2) AS avg_passengers
        FROM taxi_raw_ch
        GROUP BY hour
        ORDER BY hour
    """

    response = requests.post(
        CH_URL,
        params={"query": query},
        auth=(CH_USER, CH_PASSWORD)
    )

    if not response.ok:
        print(response.text)

    response.raise_for_status()

    print("ClickHouse: агрегация по часам готова")


# ============================================================
# TASK 4 — агрегация по дням недели
# ============================================================

@task(name="Aggregate Trips by Day of Week (ClickHouse)")
def aggregate_by_dow():
    requests.post(
        CH_URL,
        params={"query": "TRUNCATE TABLE taxi_by_dow"},
        auth=(CH_USER, CH_PASSWORD)
    ).raise_for_status()

    query = """
        INSERT INTO taxi_by_dow
        SELECT
            toDayOfWeek(pickup_datetime) AS dow,
            if(toDayOfWeek(pickup_datetime) IN (6, 7), 1, 0) AS is_weekend,
            COUNT() AS trips,
            round(avg(fare_amount), 2) AS avg_fare,
            round(
                avg(
                    greatCircleDistance(
                        pickup_longitude,
                        pickup_latitude,
                        dropoff_longitude,
                        dropoff_latitude
                    ) / 1000
                ),
                2
            ) AS avg_km,
            round(avg(passenger_count), 2) AS avg_passengers
        FROM taxi_raw_ch
        GROUP BY dow, is_weekend
        ORDER BY dow
    """

    response = requests.post(
        CH_URL,
        params={"query": query},
        auth=(CH_USER, CH_PASSWORD)
    )

    if not response.ok:
        print(response.text)

    response.raise_for_status()

    print("ClickHouse: агрегация по дням недели готова")


# ============================================================
# TASK 5 — агрегация по месяцам
# ============================================================

@task(name="Aggregate Revenue by Month (ClickHouse)")
def aggregate_by_month():
    requests.post(
        CH_URL,
        params={"query": "TRUNCATE TABLE taxi_by_month"},
        auth=(CH_USER, CH_PASSWORD)
    ).raise_for_status()

    query = """
        INSERT INTO taxi_by_month
        SELECT
            formatDateTime(pickup_datetime, '%Y-%m') AS ym,
            COUNT() AS trips,
            round(sum(fare_amount), 2) AS total_revenue,
            round(avg(fare_amount), 2) AS avg_fare,
            round(
                avg(
                    greatCircleDistance(
                        pickup_longitude,
                        pickup_latitude,
                        dropoff_longitude,
                        dropoff_latitude
                    ) / 1000
                ),
                2
            ) AS avg_km
        FROM taxi_raw_ch
        GROUP BY ym
        ORDER BY ym
    """

    response = requests.post(
        CH_URL,
        params={"query": query},
        auth=(CH_USER, CH_PASSWORD)
    )

    if not response.ok:
        print(response.text)

    response.raise_for_status()

    print("ClickHouse: агрегация по месяцам готова")


# ============================================================
# FLOW
# ============================================================

@flow(name="ClickHouse Aggregations Flow")
def clickhouse_agg_flow():
    create_clickhouse_tables()
    load_raw_data_into_clickhouse()
    aggregate_by_hour()
    aggregate_by_dow()
    aggregate_by_month()

    print("Все агрегации выполнены в ClickHouse!")


if __name__ == "__main__":
    clickhouse_agg_flow()