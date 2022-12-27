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

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", cred.access_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", cred.secret_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")

ev_bucket = "udac-evs"
ev_path = "Light_Duty_Vehicles.csv"
ev_population_path = "Electric_Vehicle_Population_Data.csv"
us_population_path = "us_population.csv"
us_state_code_path = "state_code.csv"


def create_vehicles_df():
    """
    Creating vehicles data frame and selecting needed columns
    """
    ev_df = spark.read.option("header",True).csv(f"s3a://{ev_bucket}/{ev_path}")
    return ev_df.where(col('Model').isNotNull())  \
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
def create_ev_log_df():
    """
    Creating electric vehicles logs 
    data frame and selecting needed columns
    """
    ev_pop_df = spark.read.option("header",True).csv(f"s3a://{ev_bucket}/{ev_population_path}")
    return ev_pop_df.where(col('State').isNotNull())  \
        .where(col('Model Year').isNotNull())  \
        .where(col('Make').isNotNull())  \
        .where(col('Model').isNotNull())  \
        .select(col('County').alias('county'), 
                col('State').alias('state'), 
                col('Model Year').alias('model_year'), 
                lower(col('Make')).alias('make'), 
                lower(col('Model')).alias('model'), 
                col('Electric Vehicle Type').alias('electric_vehicle_type'))



def create_states_df():
    """
    Mapping states and populations with state names then creating states
    data frame and selecting needed columns
    """
    us_pop_df = spark.read.option("header",True).csv(f"s3a://{ev_bucket}/{us_population_path}")
    us_pop_df = us_pop_df.withColumn("population", col("POPESTIMATE2019").cast('int')).drop('POPESTIMATE2019')

    total_pop = us_pop_df.agg(sum("population")).collect()[0][0]

    population_ratio = lambda state_population: round((state_population / total_pop) * 100, 1)
    population_ratio_udf = udf(population_ratio)

    us_state_df = spark.read.option("header",True).csv(f"s3a://{ev_bucket}/{us_state_code_path}")
    return us_pop_df.join(us_state_df, us_pop_df.STATE == us_state_df.state, "left")  \
                                .select(us_state_df.state, us_state_df.code, col('population'))  \
                                .withColumn('ratio', population_ratio_udf(col('population')))  \
                                .drop('STATE')  \
                                .dropDuplicates()



def extract_demographics_df():
    ev_df = create_vehicles_df()
    ev_pop_df = create_ev_log_df()
    us_state_pop_ratio_df = create_states_df()
    ev_demographics_df = ev_pop_df.join(us_state_pop_ratio_df, us_state_pop_ratio_df.code == ev_pop_df.state, "left")  \
                                    .join(ev_df, (ev_df.model_name == ev_pop_df.model) & (ev_df.year == ev_pop_df.model_year) & (ev_df.manufacturer == ev_pop_df.make), "left")  \
                                    .drop('model_year', 'model_year', 'make', 'code')

    ev_demographics_df.write.format("parquet").partitionBy('state').mode("overwrite").save(f's3a://{ev_bucket}/output')


extract_demographics_df()
