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

DAG_ID = "group2_project2"

with DAG(
    DAG_ID,
    start_date=datetime(2024, 1, 30),
    schedule_interval='0 6 * * *', # set to 6AM
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=False,
) as dag:
    # create dim table 1: SYMBOLS and set primary key (symbol)
    snowflake_create_dim_tables_1 = SnowflakeOperator(
        task_id='snowflake_create_dim_tables_1',
        sql='dim_table_1.sql',
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )
    # create dim table 2: COMPANY_PROFILE and set primary key (symbol)
    snowflake_create_dim_tables_2 = SnowflakeOperator(
       task_id='snowflake_create_dim_tables_2',
       sql='dim_table_2.sql',
       warehouse=SNOWFLAKE_WAREHOUSE,
       database=SNOWFLAKE_DATABASE,
       schema=SNOWFLAKE_SCHEMA,
       role=SNOWFLAKE_ROLE,
    )
    # create fact table : STOCK_HISTORY 
    # set primary key (symbol,date), set foreign keys (symbol) to DIM_SYMBOL_G2, DIM_COMPANY_G2
    # copy the exist records
    snowflake_create_fact_tables = SnowflakeOperator(
       task_id='snowflake_create_fact_tables',
       sql='fact_table.sql',
       warehouse=SNOWFLAKE_WAREHOUSE,
       database=SNOWFLAKE_DATABASE,
       schema=SNOWFLAKE_SCHEMA,
       role=SNOWFLAKE_ROLE,
       split_statements=True,
    )
    # update changed data based on DATE 
    snowflake_insert_fact_tables = SnowflakeOperator(
       task_id='snowflake_insert_fact_tables',
       sql='insert_fact_table.sql',
       warehouse=SNOWFLAKE_WAREHOUSE,
       database=SNOWFLAKE_DATABASE,
       schema=SNOWFLAKE_SCHEMA,
       role=SNOWFLAKE_ROLE,
    )

    # [END howto_operator_snowflake]
    snowflake_create_dim_tables_1 >> snowflake_create_fact_tables
    snowflake_create_dim_tables_2 >> snowflake_create_fact_tables
    snowflake_create_fact_tables >> snowflake_insert_fact_tables
    # [END snowflake_example_dag]

