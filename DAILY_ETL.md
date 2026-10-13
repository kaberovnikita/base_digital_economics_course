# Обработка по одному дню

Исходные таблицы `public.site_visits` и `public.user_payments` сохранены.
Только события `action_type = 'click'` выгружаются по дням в `s3://etl-source/site_visits/date=YYYY-MM-DD/data.csv`.
Flow кликов читает CSV из MinIO, flow платежей — из `public.user_payments`.
День определяется по исходному столбцу `date` без преобразования часовых поясов.

Каждый flow выбирает ровно один следующий необработанный день. Он загружает его
в базы ClickHouse `tmp.site_visits` или `tmp.user_payments`, затем заменяет
дневную партицию в ClickHouse `raw` и отмечает её в ClickHouse `etl.processed_days`.
Остальные дни ClickHouse `raw` сохраняются. Повторный запуск даты заменяет её без дубликатов.
Если доступные дни закончились, запуск успешно завершается без загрузки.
Блокировка PostgreSQL предотвращает одновременную обработку одного источника.

Выгрузить исходные клики в MinIO (включая новые дни):

```sh
docker compose exec prefect-cli python /opt/prefect/flows/export_site_visits.py
```

Запустить следующий день:

```sh
docker compose exec -w /opt/prefect/flows prefect-cli python -c 'from daily_etl import site_visits_flow; print(site_visits_flow())'
docker compose exec -w /opt/prefect/flows prefect-cli python -c 'from daily_etl import user_payments_flow; print(user_payments_flow())'
```

Повторить конкретную дату:

```sh
docker compose exec -w /opt/prefect/flows prefect-cli python -c 'from daily_etl import site_visits_flow; print(site_visits_flow("2022-06-30"))'
```

В Prefect можно передать параметр `process_date` при создании запуска.
Почасовые расписания находятся в `prefect.yaml`; worker регистрирует их при старте.
Исходные CSV должны иметь правильный заголовок, дату, SHA-256 и количество строк
в метаданных объекта — экспорт создаёт их автоматически. Существующий объект
с другим содержимым не перезаписывается автоматически; для намеренного обновления
выгрузки передайте `--replace`. Flow отклоняет CSV с просмотрами или другими событиями.

## Назначение: ClickHouse

Клики: MinIO → ClickHouse tmp.site_visits → raw.site_visits.
Платежи: PostgreSQL public.user_payments → ClickHouse tmp.user_payments → raw.user_payments.
Данные разбиты на партиции по дню; REPLACE PARTITION заменяет выбранный день без
добавления дублей. Контрольная отметка записывается после проверки публикации.
Если запись отметки не удалась, следующий запуск повторяет тот же день безопасно.
Это два последовательных действия ClickHouse, а не общая SQL-транзакция.
История отметок использует ReplacingMergeTree; для текущего состояния:

```sql
SELECT * FROM etl.processed_days FINAL ORDER BY dataset, process_date;
SELECT date, count() FROM raw.site_visits GROUP BY date ORDER BY date;
SELECT date, count() FROM raw.user_payments GROUP BY date ORDER BY date;
```

Старые схемы PostgreSQL tmp/raw/etl удалены после проверки переноса.
Таблицы tmp/raw/etl теперь находятся только в ClickHouse. PostgreSQL также предоставляет сессионную блокировку
от параллельных запусков и продолжает хранить служебную базу Prefect.

## Локальные файлы и Git

`.env`, `dump.sql`, `taxi.csv`, `minio-data/` и Python-кэш исключены из Git.
Локальные данные сохраняются на диске. Пустая копия `flows/prefect.yaml` удалена;
Compose подключает основной `prefect.yaml` из корня.

Для нового компьютера скопируйте `.env.example` в `.env` и задайте пароли.
Поместите учебный `dump.sql` в корень проекта: Compose использует его для
инициализации пустой базы. Дамп нужно получить отдельно у владельца проекта;
в репозитории он не распространяется. Он уже включает данные такси, поэтому
`taxi.csv` для восстановления не требуется. Клики можно снова выгрузить в MinIO
командой экспорта выше.

Пароль MinIO в `.env` должен совпадать с `MINIO_ROOT_PASSWORD` сервиса MinIO
в `docker-compose.yml` (текущий учебный пароль — `minio12345`).
