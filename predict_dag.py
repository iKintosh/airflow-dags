from datetime import datetime, timedelta, timezone
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
import logging
from airflow import settings
from airflow.models import Connection
from airflow.decorators import dag, task
import json


def filter_response(text):
    clean_response = []
    all_articles = json.loads(text)['items']
    for article in all_articles:
        clean_response.append(
            {
                "article": article['article'],
                "views": article['views'],
                "timestamp": parse_datetime(article['timestamp'])
            }
        )
    return json.dumps(clean_response)
        
def parse_datetime(datetime_str):
    dt = datetime.strptime(datetime_str, '%Y%m%d%H')
    return dt.date().isoformat()

    
@dag(
    dag_id="predict",
    start_date=datetime(2022, 12, 1),
    schedule_interval="@daily",
    )
def taskflow():
    
    @task(task_id='create_connection_if_not_exist')
    def task_create_connection():
        conn = Connection(conn_id="wiki_pageviews_api",
                        conn_type="HTTPS",
                        host=" https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/all-agents/",
                        description="Connection to wikimedia")
        session = settings.Session()
        conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()

        if str(conn_name) == str(conn.conn_id):
            logging.warning(f"Connection {conn.conn_id} already exists")
            return None

        session.add(conn)
        session.commit()
        logging.info(Connection.log_info(conn))
        logging.info(f'Connection is created')
        
    task_is_api_active = HttpSensor(
        task_id='is_api_active',
        http_conn_id='wiki_pageviews_api',
        endpoint='Rick_Astley/daily/20230101/20230102',
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'}
    )
    
    task_get_data = SimpleHttpOperator(
        task_id='extract_pageview',
        http_conn_id='wiki_pageviews_api',
        endpoint='Rick_Astley/daily/{{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}/{{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}',
        method='GET',
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'},
        log_response=True,
        response_filter=lambda response: filter_response(response.text)
    )
    
    task_create_connection() >> task_is_api_active >> task_get_data
    
dag = taskflow()