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

TARGET_FACT_TABLE = 'FACT_STOCK_HISTORY_GROUP3'
SOURCE_FACT_TABLE = 'SOURCE_STOCK_HISTORY_G3'

UPDATE_FACT_TABLE_SQL_STRING = (
    f"INSERT INTO {TARGET_FACT_TABLE} select * from {SOURCE_FACT_TABLE} where DATE > (SELECT MAX(DATE) FROM {TARGET_FACT_TABLE});"
)


DAG_ID = "snowflake2s_group3"
# [START howto_operator_snowflake]

with DAG(
    DAG_ID,
    start_date=datetime(2024, 1, 31),
    schedule_interval='30 08 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=True,
) as dag:
    # [START snowflake_example_dag]
    snowflake_op_sql_str = SnowflakeOperator(
        task_id='snowflake_op_sql_str',
        sql=UPDATE_FACT_TABLE_SQL_STRING,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    snowflake_op_template_file = SnowflakeOperator(
       task_id='snowflake_op_template_file',
       sql='update_target_table.sql',
       warehouse=SNOWFLAKE_WAREHOUSE,
       database=SNOWFLAKE_DATABASE,
       schema=SNOWFLAKE_SCHEMA,
       role=SNOWFLAKE_ROLE,
       split_statements=True,
    )

    # [END howto_operator_snowflake]       
    
    snowflake_op_sql_str >> snowflake_op_template_file
        
    
        
    
    # [END snowflake_example_dag]

