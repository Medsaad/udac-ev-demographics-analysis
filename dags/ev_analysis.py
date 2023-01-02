from airflow import DAG
from datetime import datetime

from includes.tasks import *

with DAG(dag_id="EV_DAG", start_date=datetime.now(), schedule_interval="@monthly", catchup=False) as dag:
    transform = transform_data(dag)
    move_to_warehouse = move_data_to_warehouse(dag)
    checks = quality_checks(dag)
    
    transform >> checks >> move_to_warehouse
