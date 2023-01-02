from datetime import timedelta

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable

def transform_data(dag):
    pyspark_app_home=Variable.get("PYSPARK_APP_HOME")

    return SparkSubmitOperator(task_id='transform_data',
        conn_id='spark_conn',
        application=f'{pyspark_app_home}/etl.py',
        total_executor_cores=1,
        executor_cores=1,
        executor_memory='2g',
        driver_memory='2g',
        name='transform_data',
        execution_timeout=timedelta(minutes=15),
        dag=dag
    )