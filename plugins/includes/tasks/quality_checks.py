from datetime import timedelta

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable

def quality_checks(dag):
    pyspark_app_home=Variable.get("PYSPARK_APP_HOME")

    return SparkSubmitOperator(task_id='quality_checks',
        conn_id='spark_conn',
        application=f'{pyspark_app_home}/quality.py',
        total_executor_cores=1,
        executor_cores=1,
        executor_memory='2g',
        driver_memory='2g',
        name='quality_checks',
        execution_timeout=timedelta(minutes=5),
        dag=dag
    )