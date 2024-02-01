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

#SNOWFLAKE_STAGE = 's3_stage_testing'


SNOWFLAKE_FACT_TABLE = 'FACT_STOCK_G2'
# SQL commands
CREATE_TABLE_SQL_STRING = (
    f"CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_FACT_TABLE}  (
  symbol varchar(16),
  date  date,
  open float,
  high float,
  low float,
  close float,
  volume float,
  adjclose float,
  FOREIGN KEY (symbol) references  DIM_COMPANY_G2 (symbol),
  FOREIGN KEY (symbol) references  DIM_SYMBOL_G2 (symbol)
);"
)


DAG_ID = "snowflake_to_snowflake_group2"
# [START howto_operator_snowflake]

with DAG(
    DAG_ID,
    start_date=datetime(2023, 1, 1),
    schedule_interval='0 5 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=False,
) as dag:
    # [START snowflake_example_dag]
    snowflake_op_sql_str = SnowflakeOperator(
        task_id='snowflake_op_sql_str',
        sql=CREATE_TABLE_SQL_STRING,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    snowflake_op_template_file = SnowflakeOperator(
       task_id='snowflake_create_dim_tables',
       sql='snowflake2snowflake.sql',
       warehouse=SNOWFLAKE_WAREHOUSE,
       database=SNOWFLAKE_DATABASE,
       schema=SNOWFLAKE_SCHEMA,
       role=SNOWFLAKE_ROLE,
       split_statements=True,
    )

    # [END howto_operator_snowflake]

    snowflake_op_template_file >> snowflake_op_sql_str

    # [END snowflake_example_dag]

