"""
Example use of Snowflake related operators.
"""
import os
from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator



SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'AIRFLOW0124'
SNOWFLAKE_SCHEMA = 'BF_DEV'

SNOWFLAKE_ROLE = 'BF_DEVELOPER0124'
SNOWFLAKE_WAREHOUSE = 'BF_ETL0124'
#SNOWFLAKE_STAGE = 'beaconfire_stage'


#SNOWFLAKE_SAMPLE_TABLE1 = 'DIM_COMPANY_GROUP1'
SNOWFLAKE_SAMPLE_TABLE = 'FACT_STOCK_HISTORY_GROUP1'
# SQL commands


DAG_ID = "snowflake_insert_fact_group1"
# [START howto_operator_snowflake]

with DAG(
    DAG_ID,
    start_date=datetime(2024, 1, 30),
    schedule_interval='0 7 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=True,
) as dag:
    
    snowflake_insert_fact_group1 = SnowflakeOperator(
       task_id='snowflake_insert_fact_group1',
       sql='fact_table_insert_date_group1.sql',
       split_statements=True,
       warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    ###Update dim table
    snowflake_insert_dim_group1 = SnowflakeOperator(
       task_id='snowflake_insert_dim_group1',
       sql='dim_table_insert_date_group1.sql',
       split_statements=True,
       warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    # [END howto_operator_snowflake]

    (
            snowflake_insert_fact_group1,
            snowflake_insert_dim_group1
    )
    # [END snowflake_example_dag]

