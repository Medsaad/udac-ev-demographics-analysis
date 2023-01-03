# EV Analytics

This analytics DAG extracts EV purchases data across united states in 2020 and clean these data and add them together in one Redshift table to be ready for geographic analytics.

# Provided Data

You can find provided data below under `data` folder. Please upload them to your AWS S3 in a bucket named `udac-evs`.
- `Electric_Vehicle_Population_Data`: logging all EV bought within the year 2020
- `Light_Duty_Vehicles`: listing all EVs models and specs.
- `us_population`: contains USA population by state.
- `state_code`: maps each state name with its code.

## Input data schema

The input data has a snowflake schema since the us population table needed to be joined with us state code table.

![Input Data](https://drive.google.com/uc?id=1NfeOGWalJv24arqy7Vn64HJejNM4P0f6)

For this study, I think monthly input updates would be optimal to get up to date results.

**Notes**

- In case of huge spike of EV adoption in the coming years, the population input code increase dramatically. Then it would be better to switch spark from local mode to a standalone mode with multiple executors and cores in place. It can be self hosted on AWS EC2 or using AWS EMR.
- Since the increase will only happen in the `Electric_Vehicle_Population_Data` table. It may be helpful in the future if this proccessing part was split out and started in paralell with other tables processing and then all of them got merged together after the quality check.
- Once there are enough up-to-date data enough for a daily DAG runs, that will not have a huge technical impact other than the spike of if cloud resources usage and cost.
- At the time of writing this docs, AWS allows up to 500 redshift connections. There are up to 50 concurrent queries and anything above that will be queued until one of the slots is empty.

## Output data schema

The output schema got denormalized so that analytics team can extract insights without the need for joins. You can find the exported columns at the end of this file.

# Tools Used

In this project, I've used a set of tools to get this job done:

- **AWS S3**: used as the project state storage tool from input data storage to a medium between steps.
- **AWS Redshift**: used as the analytics tool and the residence of the final shape of data.
- **Apache Airflow**: divides the project steps to logical modules to facilitate error tracking and improve reusablity.
- **Apache Spark**: A tool to transform data and process it .. also used for all analytics checks.

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

- **Note**: There is a python notebook in this repo to work through the steps above seperately.

- Next step extracts data from S3 and copy it to a created redshift table `vehicles_analytics`.

- Check [this sample result](data/sample/ev_logs_by_city.csv) when querying EV distribution by city. You can also find this inside the python notebook.

- Here is a data dictionary of the output:

| Column | Data Type | Example | Source |
|:------:|:------:   | :------:|:------:|
| state | string | "WA"  | us_population |
| model_name | string | "optima"  | Light_Duty_Vehicles |
| electric_vehicle_type | string | "Battery Electric Vehicle"  | Light_Duty_Vehicles |
| model_year | number | 2011  | Light_Duty_Vehicles |
| manufacturer | string | "Audi"  | ev_population |
| population | number | 20,123 | us_population |
| ratio | float | 4.1  | (generated) |
| transmission_type | string | "Auto"  | Light_Duty_Vehicles |
| engine_type | string | "SI"  | Light_Duty_Vehicles |
| engine_size | string | "3.5L"  | Light_Duty_Vehicles |
| category | string | "Sedan/Wagon"  | Light_Duty_Vehicles |
| fuel | string | "Hybrid Electric"  | Light_Duty_Vehicles |
