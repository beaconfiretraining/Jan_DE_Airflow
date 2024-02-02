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

DAG_ID = "group5_project2"
# [START howto_operator_snowflake]

with DAG(
    DAG_ID,
    start_date=datetime(2024, 1, 31),
    schedule_interval='0 23 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire_group5'],
    catchup=False,
) as dag:
    # [START snowflake_example_dag]
    snowflake_update_dim = SnowflakeOperator(
        task_id='snowflake_update_dim',
        sql="dim_update.sql",
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )
    
    snowflake_create_2 = SnowflakeOperator(
        task_id='snowflake_create_2',
        sql="fact.sql",
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )
    
    snowflake_insert_2 = SnowflakeOperator(
        task_id='snowflake_insert_2',
        sql="fact_insert.sql",
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )


    # [END howto_operator_snowflake]


    (
        snowflake_update_dim,
        snowflake_create_2
        >> snowflake_insert_2
    )
    # [END snowflake_example_dag]
