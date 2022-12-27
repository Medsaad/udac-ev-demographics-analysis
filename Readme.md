# EV Analytics

This analytics DAG extracts EV purchases dta across united states and clean these data and add them together in one Redshift table to be ready for geographic analytics.

# Provided Data

You can find provided data below under `data` folder. Please upload them to your AWS S3 in a bucket named `udac-evs`.
- `Electric_Vehicle_Population_Data`: logging all EV bought within the year 2020
- `Light_Duty_Vehicles`: listing all EVs models and specs.
- `us_population`: contains USA population by state.
- `state_code`: maps each state name with its code.

# How To setup

- Create a redshift cluster and then create a new table `vehicles_analytics`
- Make sure you have both Spark v3.3.0 & Airflow v2.5.0 are installed in your machine.
- Install Airflow plugins for AWS, Postgres & Spark.
- Setup path to your project by running the following: `export AIRFLOW_VAR_PYSPARK_APP_HOME=<path/to/project>`
- Set the following:
  - redshift connection `redshift-conn`
  - AWS connection `udac-conn`
  - Spark connection `spark_conn`
- Run Airflow, then run DAG.

# How this works

- Spark extracts data from S3 bucket

- Clean and transform the data, insure data quality then join them using state, car model, model year and manufacturer.

- After transformation data gets uploaded again in the same bucket inside `output` folder.

- Next step extracts data from S3 and copy it to a created redshift table `vehicles_analytics`.

- The final table will include the following columns:
  - country
  - state
  - model
  - electric_vehicle_type
  - population
  - ratio
  - model_name
  - year
  - transmission_type
  - engine_type
  - engine_size
  - manufacturer
  - category
  - fuel
