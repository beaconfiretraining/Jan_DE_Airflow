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




SNOWFLAKE_dim_TABLE = 'dim_TableName_Group5'
SNOWFLAKE_fact_TABLE = 'dim_TableName_Group5'
# SQL commands
CREATE_TABLE_SQL_STRING_1 = (
    f"CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_dim_TABLE} ();"
)
CREATE_TABLE_SQL_STRING_2 = (
    f"CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_fact_TABLE} ();"
)
SQL_INSERT_STATEMENT_1 = f"INSERT INTO {SNOWFLAKE_dim_TABLE} "
SQL_INSERT_STATEMENT_2 = f"INSERT INTO {SNOWFLAKE_fact_TABLE} "



DAG_ID = "group5_project5"
# [START howto_operator_snowflake]

with DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule_interval='30 * * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire_group5'],
    catchup=False,
) as dag:
    # [START snowflake_example_dag]
    snowflake_create_1 = SnowflakeOperator(
        task_id='snowflake_create_1',
        sql=CREATE_TABLE_SQL_STRING_1,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    snowflake_insert_1 = SnowflakeOperator(
        task_id='snowflake_insert_1',
        sql=SQL_INSERT_STATEMENT_1,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    snowflake_create_2 = SnowflakeOperator(
        task_id='snowflake_create_2',
        sql=CREATE_TABLE_SQL_STRING_2,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    snowflake_insert_2 = SnowflakeOperator(
        task_id='snowflake_insert_2',
        sql=SQL_INSERT_STATEMENT_2,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    

    # [END howto_operator_snowflake]


    (
        snowflake_create_1
        >> snowflake_insert_1
        ,snowflake_create_2
        >> snowflake_insert_2
    )
    # [END snowflake_example_dag]

