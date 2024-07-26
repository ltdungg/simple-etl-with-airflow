from airflow.decorators import dag, task
from etl.extract import _extract
from datetime import datetime, timedelta
from etl.transform import _transform_and_load
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'Dzung Luong',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}
@dag(
    dag_id="simple_etl",
    default_args=default_args,
    start_date=datetime(2024, 7, 24),
    schedule_interval="@daily",
    catchup=False
)
def my_simple_etl():
    @task
    def extracting():
        _extract()
    
    @task
    def transform_and_load():
        _transform_and_load()
    
    extracting()
    transform_and_load()
    
run_dag = my_simple_etl()