"""One source day per run, with staging and atomic raw replacement."""
import csv
import hashlib
import io
import json
import time
import requests
import os
import re
from datetime import date

import boto3
import psycopg
from botocore.config import Config
from prefect import flow, task, get_run_logger

DATASETS = ('site_visits', 'user_payments')
BUCKET = os.getenv('MINIO_BUCKET', 'etl-source')


def db():
    host, _, port = os.getenv('POSTGRES_HOST', 'postgres:5432').partition(':')
    return psycopg.connect(host=host, port=int(port or 5432),
                           dbname=os.environ['POSTGRES_DB'],
                           user=os.environ['POSTGRES_USER'],
                           password=os.environ['POSTGRES_PASSWORD'])


def s3():
    return boto3.client('s3', endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://minio:9000'),
                        aws_access_key_id=os.environ['MINIO_ROOT_USER'],
                        aws_secret_access_key=os.environ['MINIO_ROOT_PASSWORD'],
                        region_name='us-east-1',
                        config=Config(signature_version='s3v4', connect_timeout=10, read_timeout=60))


COLUMNS = {
    'site_visits': ['date', 'timestamp', 'user_client_id', 'action_type', 'placement_type', 'placement_id', 'user_visit_url', 'load_date'],
    'user_payments': ['date', 'timestamp', 'user_client_id', 'item', 'price', 'quantity', 'amount', 'discount', 'order_id', 'status'],
}
DDL = {
    'site_visits': """date Date, timestamp Nullable(String), user_client_id Nullable(Int32),
        action_type String, placement_type Nullable(String), placement_id Nullable(Int32),
        user_visit_url Nullable(String), load_date Nullable(String)""",
    'user_payments': """date Date, timestamp DateTime('UTC'), user_client_id Int32,
        item String, price Int32, quantity Int32, amount Float64, discount Float64,
        order_id Int32, status String""",
}


def ch(query, data=None, **parameters):
    response = requests.post(os.environ['CH_URL'],
        params={'query': query, **{'param_' + k: v for k, v in parameters.items()}},
        data=data, auth=(os.environ['CLICKHOUSE_USER'], os.environ['CLICKHOUSE_PASSWORD']),
        timeout=(10, 120))
    if not response.ok:
        raise RuntimeError(f'ClickHouse: {response.text}')
    return response


def query_rows(query, **parameters):
    return ch(query + ' FORMAT JSON', **parameters).json()['data']


def setup(dataset):
    if dataset not in DATASETS:
        raise ValueError('Unknown dataset')
    for database in ('tmp', 'raw', 'etl'):
        ch(f'CREATE DATABASE IF NOT EXISTS {database}')
    for database in ('tmp', 'raw'):
        ch(f'CREATE TABLE IF NOT EXISTS {database}.{dataset} ({DDL[dataset]}) '
           'ENGINE=MergeTree() PARTITION BY date ORDER BY date')
    ch("""CREATE TABLE IF NOT EXISTS etl.processed_days (
        dataset String, process_date Date, row_count UInt64,
        completed_at DateTime64(3, 'UTC'), version UInt64
        ) ENGINE=ReplacingMergeTree(version) ORDER BY (dataset, process_date)""")


@task(name='Select one unprocessed day')
def select_day(dataset, process_date=None):
    if process_date is not None:
        return date.fromisoformat(process_date).isoformat()
    completed = {date.fromisoformat(r['process_date']) for r in query_rows(
        'SELECT process_date FROM etl.processed_days FINAL WHERE dataset={dataset:String}', dataset=dataset)}
    if dataset == 'site_visits':
        available = set()
        for page in s3().get_paginator('list_objects_v2').paginate(Bucket=BUCKET, Prefix='site_visits/'):
            for obj in page.get('Contents', []):
                match = re.fullmatch(r'site_visits/date=(\d{4}-\d{2}-\d{2})/data.csv', obj['Key'])
                if match:
                    available.add(date.fromisoformat(match[1]))
    else:
        with db() as conn:
            available = {date.fromisoformat(r[0]) for r in conn.execute(
                'SELECT DISTINCT date FROM public.user_payments')}
    remaining = sorted(available - completed)
    return remaining[0].isoformat() if remaining else None


@task(name='Load selected day into ClickHouse tmp')
def load_tmp(dataset, day):
    if dataset == 'site_visits':
        obj = s3().get_object(Bucket=BUCKET, Key=f'site_visits/date={day}/data.csv')
        body = obj['Body'].read()
        if hashlib.sha256(body).hexdigest() != obj.get('Metadata', {}).get('sha256'):
            raise ValueError('Missing or invalid source SHA256')
        reader = csv.DictReader(io.StringIO(body.decode('utf-8')))
        if reader.fieldnames != COLUMNS[dataset]:
            raise ValueError('CSV columns do not match site_visits')
        rows = list(reader)
        for row in rows:
            if row['date'] != day or row['action_type'] != 'click':
                raise ValueError('CSV must contain only clicks for the selected day')
            for field in ('user_client_id', 'placement_id'):
                row[field] = int(row[field]) if row[field] else None
        if len(rows) != int(obj['Metadata']['rows']):
            raise ValueError('Source row count mismatch')
    else:
        with db() as conn:
            cursor = conn.execute('SELECT * FROM public.user_payments WHERE date=%s', (day,))
            columns = [c.name for c in cursor.description]
            if columns != COLUMNS[dataset]:
                raise ValueError('PostgreSQL columns do not match user_payments')
            rows = [dict(zip(columns, row)) for row in cursor]
            for row in rows:
                row['timestamp'] = row['timestamp'].isoformat(sep=' ')
    if not rows:
        raise ValueError(f'No {dataset} data for {day}; raw remains unchanged')
    body = ''.join(json.dumps(row, ensure_ascii=False, allow_nan=False) + '\n' for row in rows).encode()
    ch(f'TRUNCATE TABLE tmp.{dataset}')
    ch(f'INSERT INTO tmp.{dataset} FORMAT JSONEachRow', data=body)
    count = int(query_rows(f'SELECT count() AS n FROM tmp.{dataset} WHERE date={{day:Date}}', day=day)[0]['n'])
    total = int(query_rows(f'SELECT count() AS n FROM tmp.{dataset}')[0]['n'])
    if count != len(rows) or total != count:
        raise ValueError('ClickHouse staging row count mismatch')
    return count


@task(name='Publish tmp day into ClickHouse raw')
def publish_raw(dataset, day, expected_rows):
    count = int(query_rows(f'SELECT count() AS n FROM tmp.{dataset} WHERE date={{day:Date}}', day=day)[0]['n'])
    if count != expected_rows:
        raise ValueError('Staging changed before publication')
    # Replace the entire daily partition. Retries replace it again rather than append.
    ch(f'ALTER TABLE raw.{dataset} REPLACE PARTITION {{day:Date}} FROM tmp.{dataset}', day=day)
    actual = int(query_rows(f'SELECT count() AS n FROM raw.{dataset} WHERE date={{day:Date}}', day=day)[0]['n'])
    if actual != count:
        raise ValueError('Published row count mismatch; day not marked processed')
    # If this write fails, the next run retries the same day safely.
    ch("""INSERT INTO etl.processed_days SELECT {dataset:String}, {day:Date},
        {rows:UInt64}, now64(3), {version:UInt64}""",
        dataset=dataset, day=day, rows=count, version=time.time_ns())
    return {'dataset': dataset, 'process_date': day, 'rows': count, 'destination': 'ClickHouse'}


def run_dataset(dataset, process_date):
    # PostgreSQL provides only a session lock; staging, raw and checkpoints are in ClickHouse.
    with db() as lock_conn:
        lock_conn.autocommit = True
        lock_conn.execute('SELECT pg_advisory_lock(hashtext(%s))', ('daily-etl:' + dataset,))
        try:
            setup(dataset)
            day = select_day(dataset, process_date)
            if day is None:
                get_run_logger().info('All available days for %s have been processed', dataset)
                return {'dataset': dataset, 'status': 'no_new_days'}
            result = publish_raw(dataset, day, load_tmp(dataset, day))
            get_run_logger().info('Published %s', result)
            return result
        finally:
            lock_conn.execute('SELECT pg_advisory_unlock(hashtext(%s))', ('daily-etl:' + dataset,))


@flow(name='Site Visits Daily ETL')
def site_visits_flow(process_date: str | None = None):
    return run_dataset('site_visits', process_date)


@flow(name='User Payments Daily ETL')
def user_payments_flow(process_date: str | None = None):
    return run_dataset('user_payments', process_date)
