from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

S3_BUCKET="ev-udac-proj"
S3_PREFIX="output"

def move_data_to_warehouse(dag):
    return S3ToRedshiftOperator(
        s3_bucket=S3_BUCKET,
        s3_key=f"{S3_PREFIX}",
        schema="public",
        table="vehicles",
        copy_options=['parquet'],
        task_id='move_data_to_warehouse',
        dag=dag
        )