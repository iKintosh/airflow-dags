import json
from datetime import datetime
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import logging
from airflow import settings
from airflow.models import Connection
from airflow.decorators import dag, task

    
@dag(
    dag_id="predict", 
    schedule=None, 
    start_date=datetime(2023, 1, 1), 
    params={
        "start_date": (datetime.datetime.now().date() - datetime.timedelta(1)).strftime("%Y%m%d"), 
        "end_date": datetime.datetime.now().date().strftime("%Y%m%d")
        }
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

        session.add(conn)
        session.commit()
        logging.info(Connection.log_info(conn))
        logging.info(f'Connection is created')
        
    task_is_api_active = HttpSensor(
        task_id='is_api_active',
        http_conn_id='wiki_pageviews_api',
        endpoint='Rick_Astley/daily/20230101/20230102'
    )
    
    task_get_data = SimpleHttpOperator(
        task_id='extract_pageview',
        http_conn_id='wiki_pageviews_api',
        endpoint='Rick_Astley/daily/{{ params.start_date }}/{{ params.end_date }}',
        method='GET',
    )
    
    task_create_connection() >> task_is_api_active >> task_get_data
    
dag = taskflow()