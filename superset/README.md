# Superset для ClickHouse

```sh
docker compose up -d --build superset
```

Интерфейс: http://localhost:8088
Дашборд: http://localhost:8088/superset/dashboard/clickhouse-analytics/
Пользователь: `admin`. Пароль берётся из `SUPERSET_ADMIN_PASSWORD` в локальном `.env`.
При первом запуске создаются пользователь, подключение ClickHouse ETL, четыре
датасета и восемь графиков. При повторном запуске они обновляются без дубликатов.
Изменение пароля в `.env` не меняет пароль уже созданного пользователя.

Superset читает ClickHouse напрямую. Клики и платежи показываются только за
дни, уже обработанные Prefect. Суммы платежей включают все статусы заказов;
это не показатель только завершённых продаж. Дашборд обновляется каждые пять минут.
Статистика такси использует отдельные агрегированные таблицы.

Служебные данные BI (пользователи, подключение, графики, дашборд) находятся в
отдельном PostgreSQL `superset-db`, в Docker-томе `superset_db_data`.
Код инициализации и настройки — в этой папке. SECRET_KEY хранится только в `.env`.
Первичная инициализация предполагает, что таблицы raw.site_visits,
raw.user_payments, default.taxi_by_hour и default.taxi_by_month уже созданы.

Используется Superset 4.1.2 с ClickHouse Connect 0.8.18. На Apple Silicon
образ запускается через Docker с `platform: linux/amd64`.
