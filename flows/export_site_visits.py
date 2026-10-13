"""Publish the course click data to MinIO as one CSV object per source day."""
import argparse
import hashlib
import io
from botocore.exceptions import ClientError
from psycopg import sql
from daily_etl import BUCKET, db, s3


def export(replace=False):
    client = s3()
    try:
        client.head_bucket(Bucket=BUCKET)
    except ClientError as exc:
        if exc.response['ResponseMetadata']['HTTPStatusCode'] != 404:
            raise
        client.create_bucket(Bucket=BUCKET)
    total = 0
    with db() as conn:
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        days = conn.execute("SELECT date, count(*) FROM public.site_visits WHERE action_type = 'click' GROUP BY date ORDER BY date").fetchall()
        for day, rows in days:
            buffer = io.BytesIO()
            query = sql.SQL("COPY (SELECT * FROM public.site_visits WHERE date={} AND action_type='click' ORDER BY timestamp, user_client_id, action_type, placement_type, placement_id, user_visit_url, load_date) TO STDOUT WITH (FORMAT CSV, HEADER TRUE)").format(sql.Literal(day))
            with conn.cursor().copy(query) as copy:
                for block in copy:
                    buffer.write(block)
            data = buffer.getvalue()
            sha = hashlib.sha256(data).hexdigest()
            key = f'site_visits/date={day}/data.csv'
            try:
                existing = client.head_object(Bucket=BUCKET, Key=key)
            except ClientError as exc:
                if exc.response['ResponseMetadata']['HTTPStatusCode'] != 404:
                    raise
                client.put_object(Bucket=BUCKET, Key=key, Body=data, ContentType='text/csv',
                                  Metadata={'sha256': sha, 'rows': str(rows), 'action-type': 'click'})
            else:
                if existing.get('Metadata', {}).get('sha256') != sha:
                    if not replace:
                        raise RuntimeError(f'{key} exists with different data; use --replace for an intentional refresh')
                    client.put_object(Bucket=BUCKET, Key=key, Body=data, ContentType='text/csv',
                                      Metadata={'sha256': sha, 'rows': str(rows), 'action-type': 'click'})
            restored = client.get_object(Bucket=BUCKET, Key=key)['Body'].read()
            assert hashlib.sha256(restored).hexdigest() == sha
            total += rows
            print(f'{day}: {rows} rows, verified')
    print(f'Exported {len(days)} days, {total} rows to s3://{BUCKET}/site_visits/')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--replace', action='store_true', help='Replace existing daily CSV files')
    export(replace=parser.parse_args().replace)
