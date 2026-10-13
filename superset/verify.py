"""Smoke-test authentication, dashboard layout and every chart's ClickHouse query."""
import json
import os
import requests
from superset.app import create_app

app=create_app()
with app.app_context():
    from superset.models.dashboard import Dashboard
    from superset import db
    dashboard=db.session.query(Dashboard).filter_by(slug='clickhouse-analytics').one()
    charts=[]
    for chart in dashboard.slices:
        params=json.loads(chart.params)
        if chart.viz_type.startswith('echarts_timeseries'):
            assert params.get('x_axis') not in params.get('groupby', []), chart.slice_name
        charts.append((chart.slice_name,json.loads(chart.query_context)))

session=requests.Session()
base='http://localhost:8088'
r=session.post(base+'/api/v1/security/login',json={'username':'admin','password':os.environ['SUPERSET_ADMIN_PASSWORD'],'provider':'db','refresh':True},timeout=30)
r.raise_for_status()
session.headers['Authorization']='Bearer '+r.json()['access_token']
r=session.get(base+'/api/v1/security/csrf_token/',timeout=30)
r.raise_for_status()
session.headers['X-CSRFToken']=r.json()['result']
for title,context in charts:
    r=session.post(base+'/api/v1/chart/data',json=context,timeout=120)
    if r.status_code!=200:
        raise RuntimeError(f'{title}: HTTP {r.status_code}: {r.text}')
    results=r.json()['result']
    for result in results:
        if result.get('error') or result.get('status')=='failed':
            raise RuntimeError(f'{title}: {result}')
        if not result.get('data'):
            raise RuntimeError(f'{title}: empty chart')
    print('VERIFIED:',title,'rows:',len(results[0]['data']),flush=True)
print('All 8 chart queries and admin authentication passed',flush=True)
