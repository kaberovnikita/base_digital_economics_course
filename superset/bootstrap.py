"""Idempotently provision a local admin, ClickHouse datasets and a dashboard."""
import json
import os
from urllib.parse import quote_plus
from superset.app import create_app
from superset import db, security_manager

app = create_app()
with app.app_context():
    from superset.models.core import Database
    from superset.connectors.sqla.models import SqlaTable
    from superset.models.slice import Slice
    from superset.models.dashboard import Dashboard
    admin = security_manager.find_user(username='admin')
    if not admin:
        admin = security_manager.add_user('admin', 'Local', 'Admin', 'admin@localhost.local',
            security_manager.find_role('Admin'), os.environ['SUPERSET_ADMIN_PASSWORD'])
    database = db.session.query(Database).filter_by(database_name='ClickHouse ETL').one_or_none()
    if database is None:
        database = Database(database_name='ClickHouse ETL')
        db.session.add(database)
    database.sqlalchemy_uri = ('clickhousedb://' + quote_plus(os.environ['CLICKHOUSE_USER']) + ':' +
        quote_plus(os.environ['CLICKHOUSE_PASSWORD']) + '@clickhouse:8123/default')
    database.expose_in_sqllab = True
    database.allow_dml = False
    db.session.flush()
    tables = {}
    for schema, name in [('raw','site_visits'),('raw','user_payments'),
                         ('default','taxi_by_hour'),('default','taxi_by_month')]:
        table = db.session.query(SqlaTable).filter_by(database_id=database.id, schema=schema, table_name=name).one_or_none()
        if table is None:
            table = SqlaTable(database_id=database.id, schema=schema, table_name=name, owners=[admin])
            db.session.add(table)
            db.session.flush()
        table.fetch_metadata()
        tables[name] = table
    db.session.flush()

    specs = [
        ('Клики: всего','site_visits','big_number_total',None,'COUNT(*)','Клики'),
        ('Платежи: записей','user_payments','big_number_total',None,'COUNT(*)','Платежи'),
        ('Клики по дням','site_visits','echarts_timeseries_bar','date','COUNT(*)','Клики'),
        ('Сумма платежей по дням (все статусы)','user_payments','echarts_timeseries_line','date','SUM(amount)','Сумма'),
        ('Сумма платежей по товарам (все статусы)','user_payments','echarts_timeseries_bar','item','SUM(amount)','Сумма'),
        ('Платежи по статусам','user_payments','pie','status','COUNT(*)','Платежи'),
        ('Поездки такси по часам','taxi_by_hour','echarts_timeseries_bar','hour','SUM(trips)','Поездки'),
        ('Поездки такси по месяцам','taxi_by_month','echarts_timeseries_line','ym','SUM(trips)','Поездки'),
    ]
    charts=[]
    for title, table_name, viz, dimension, expression, label in specs:
        table=tables[table_name]
        metric={'expressionType':'SQL','sqlExpression':expression,'label':label,
                'optionName':'metric_'+table_name+'_'+expression.replace('(','').replace(')','')}
        params={'viz_type':viz,'datasource':f'{table.id}__table','metrics':[metric],
                'metric':metric,'groupby':[dimension] if viz == 'pie' else [],
                'x_axis':dimension,'time_range':'No filter','row_limit':1000,
                'show_legend':True,'show_labels':True,'color_scheme':'supersetColors',
                'y_axis_format':',.0f','number_format':',.0f','order_desc':False,
                'rich_tooltip':True,'annotation_layers':[]}
        context={'datasource':{'id':table.id,'type':'table'},'force':False,
                 'queries':[{'columns':[dimension] if dimension else [],'metrics':[metric],
                    'filters':[],'row_limit':1000,'time_range':'No filter',
                    'orderby':[[dimension,True]] if dimension else [],
                    'extras':{'having':'','where':''}}],
                 'form_data':params,'result_format':'json','result_type':'full'}
        chart=db.session.query(Slice).filter_by(slice_name=title).one_or_none()
        if chart is None:
            chart=Slice(slice_name=title,owners=[admin])
            db.session.add(chart)
        chart.datasource_id=table.id
        chart.datasource_type='table'
        chart.viz_type=viz
        chart.params=json.dumps(params,ensure_ascii=False)
        chart.query_context=json.dumps(context,ensure_ascii=False)
        charts.append(chart)
    db.session.flush()
    dashboard=db.session.query(Dashboard).filter_by(slug='clickhouse-analytics').one_or_none()
    if dashboard is None:
        dashboard=Dashboard(dashboard_title='ClickHouse — клики, платежи и такси',slug='clickhouse-analytics',owners=[admin])
        db.session.add(dashboard)
    dashboard.published=True
    dashboard.slices=charts
    layout={
        'DASHBOARD_VERSION_KEY':'v2',
        'ROOT_ID':{'id':'ROOT_ID','type':'ROOT','children':['GRID_ID']},
        'GRID_ID':{'id':'GRID_ID','type':'GRID','children':[],'parents':['ROOT_ID']},
        'HEADER_ID':{'id':'HEADER_ID','type':'HEADER','meta':{'text':dashboard.dashboard_title}},
    }
    for row_index in range(4):
        row_id=f'ROW-{row_index}'
        layout['GRID_ID']['children'].append(row_id)
        layout[row_id]={'id':row_id,'type':'ROW','children':[], 'parents':['ROOT_ID','GRID_ID'], 'meta':{'background':'BACKGROUND_TRANSPARENT'}}
        for chart in charts[row_index*2:row_index*2+2]:
            chart_id=f'CHART-{chart.id}'
            layout[row_id]['children'].append(chart_id)
            layout[chart_id]={'id':chart_id,'type':'CHART','children':[],
                'parents':['ROOT_ID','GRID_ID',row_id],
                'meta':{'chartId':chart.id,'sliceName':chart.slice_name,'width':6,'height':45}}
    dashboard.position_json=json.dumps(layout,ensure_ascii=False)
    dashboard.json_metadata=json.dumps({'refresh_frequency':300,'color_scheme':'supersetColors'})
    db.session.commit()
    print('Provisioned ClickHouse connection, 4 datasets and 8 charts: /superset/dashboard/clickhouse-analytics/')
