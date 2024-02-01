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




DAG_ID = "snowflake2s_group3"
# [START howto_operator_snowflake]

with DAG(
    DAG_ID,
    start_date=datetime(2024, 1, 31),
    schedule_interval='30 * * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=False,
) as dag:
    # [START snowflake_example_dag]
    

    snowflake_op_template_file = SnowflakeOperator(
       task_id='snowflake_op_template_file',
       sql='beaconfire_dev_db_test.sql',
       warehouse=SNOWFLAKE_WAREHOUSE,
       database=SNOWFLAKE_DATABASE,
       schema=SNOWFLAKE_SCHEMA,
       role=SNOWFLAKE_ROLE,
       split_statements=True,
    )

    # [END howto_operator_snowflake]       
    snowflake_op_template_file
        
    
    # [END snowflake_example_dag]

