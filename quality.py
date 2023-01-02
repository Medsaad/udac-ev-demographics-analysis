from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


##############################################
#           QUALITY FUNCTIONS                #
##############################################

def output_contains_core_columns(ev_demographics_load_df):
    core_columns = set(['state', 'model_name', 'model_year', 'manufacturer', 'population', 'ratio'])
    if not core_columns.issubset(set(ev_demographics_load_df.columns)):
        raise Exception('One Or More Core Columns are missing!')

def output_should_not_contain_nullable_values_on_core_values(ev_demographics_load_df):
    nullable_count = ev_demographics_load_df.filter((col('state').isNull()) | 
                                           (col('model_name').isNull()) | 
                                           (col('model_year').isNull()) | 
                                           (col('manufacturer').isNull()))
    if nullable_count.count() != 0:
        raise Exception('One Or More Null Values!')

##############################################
#           SETUP & CONFIGS                  #
##############################################
aws = AwsBaseHook(aws_conn_id='udac-conn')
cred = aws.get_credentials()

spark = SparkSession \
        .builder \
        .appName("Wrangling Data") \
        .getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", cred.access_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", cred.secret_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")

ev_bucket = "udac-evs"

ev_demographics_load_df = spark.read.parquet(f's3a://{ev_bucket}/output')

output_contains_core_columns(ev_demographics_load_df)
output_should_not_contain_nullable_values_on_core_values(ev_demographics_load_df)