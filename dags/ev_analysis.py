from airflow import DAG
from datetime import datetime

from includes.tasks import *

with DAG(dag_id="EV_DAG", start_date=datetime.now(), schedule_interval="@hourly", catchup=False) as dag:
    transform = transform_data(dag)
    move_to_warehouse = move_data_to_warehouse(dag)
    
    transform >> move_to_warehouse
