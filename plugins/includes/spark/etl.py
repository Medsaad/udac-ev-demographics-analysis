from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, sum, col, lower
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

##############################################
#           SETUP & CONFIGS                  #
##############################################
aws = AwsBaseHook(aws_conn_id='udac-conn')
cred = aws.get_credentials()

spark = SparkSession \
        .builder \
        .appName("Wrangling Data") \
        .getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", cred.aws_access_key_id)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", cred.aws_secret_access_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")

ev_bucket = "udac-evs"
ev_path = "Light_Duty_Vehicles.csv"
ev_population_path = "Electric_Vehicle_Population_Data.csv"
us_population_path = "us_population.csv"
us_state_code_path = "state_code.csv"

##############################################
#              EV VEHACLES DATA              #
##############################################
ev_df = spark.read.option("header",True).csv(f"s3a://{ev_bucket}/{ev_path}")
ev_df = ev_df.where(col('Model').isNotNull())  \
    .where(col('Model Year').isNotNull())  \
    .where(col('Manufacturer').isNotNull())  \
    .select(
    lower(col('Model')).alias('model_name'), 
    col('Model Year').alias('year'), 
    col('Transmission Type').alias('transmission_type'), 
    col('Engine Type').alias('engine_type'), 
    col('Engine Size').alias('engine_size'), 
    lower(col('Manufacturer')).alias('manufacturer'), 
    col('Category').alias('category'), 
    col('Fuel').alias('fuel')) \
    .dropDuplicates()

##############################################
#           EV POPULATION  DATA              #
##############################################
ev_pop_df = spark.read.option("header",True).csv(f"s3a://{ev_bucket}/{ev_population_path}")
ev_pop_df = ev_pop_df.where(col('State').isNotNull())  \
    .where(col('Model Year').isNotNull())  \
    .where(col('Make').isNotNull())  \
    .where(col('Model').isNotNull())  \
    .select(col('County').alias('county'), 
            col('State').alias('state'), 
            col('Model Year').alias('model_year'), 
            lower(col('Make')).alias('make'), 
            lower(col('Model')).alias('model'), 
            col('Electric Vehicle Type').alias('electric_vehicle_type'))


##############################################
#           US POPULATION  DATA              #
##############################################
us_pop_df = spark.read.option("header",True).csv(f"s3a://{ev_bucket}/{us_population_path}")
us_pop_df = us_pop_df.withColumn("population", col("POPESTIMATE2019").cast('int')).drop('POPESTIMATE2019')

total_pop = us_pop_df.agg(sum("population")).collect()[0][0]

def population_ratio(state_population):
    return round((state_population / total_pop) * 100, 1)
 
population_ratio_udf = udf(population_ratio)

us_state_df = spark.read.option("header",True).csv(f"s3a://{ev_bucket}/{us_state_code_path}")
us_state_pop_ratio_df = us_pop_df.join(us_state_df, us_pop_df.STATE == us_state_df.state, "left")  \
                            .select(us_state_df.state, us_state_df.code, col('population'))  \
                            .withColumn('ratio', population_ratio_udf(col('population')))  \
                            .drop('STATE')  \
                            .dropDuplicates()


##############################################
#           CONSOLIDATING OUTPUT             #
##############################################
ev_demographics_df = ev_pop_df.join(us_state_pop_ratio_df, us_state_pop_ratio_df.code == ev_pop_df.state, "left")  \
                                .join(ev_df, (ev_df.model_name == ev_pop_df.model) & (ev_df.year == ev_pop_df.model_year) & (ev_df.manufacturer == ev_pop_df.make), "left")  \
                                .drop('model_year', 'model_year', 'make', 'code')

ev_demographics_df.write.format("parquet").partitionBy('state').mode("overwrite").save(f's3a://{ev_bucket}/output')
