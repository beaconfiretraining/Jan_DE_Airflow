"""
Example use of Snowflake related operators.
"""

#!pip install apache-airflow==2.1.1
#!pip install apache-airflow-providers-snowflake==2.1.1

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'AIRFLOW0124'
SNOWFLAKE_SCHEMA = 'BF_DEV'

SNOWFLAKE_ROLE = 'BF_DEVELOPER0124'
SNOWFLAKE_WAREHOUSE = 'BF_ETL0124'

SNOWFLAKE_STAGE = 'S3_STAGE_TRANS_ORDER' 
# S3_FILE_PATH = 'transactions_group4_20240130.csv'

with DAG(
    "s3_to_snowflake_group2_test",
    start_date=datetime(2023, 11, 28),
    schedule_interval='0 6 * * *', # set to 6AM
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=True, # keep running
) as dag:

    copy_into_prestg = S3ToSnowflakeOperator(
        task_id='prestage_product_order_trans_group2', 
        # run this to add previous data
        # s3_keys=[ 'transactions_group4_20240130.csv' ],
        # s3_keys=[ 'transactions_group4_20240131.csv' ],

        # then run this make the pipeline keep running when new data coming
        s3_keys=['transactions_group4_{{ ds[0:4]+ds[5:7]+ds[8:10] }}.csv'],

        table='prestage_product_order_trans_group2', 
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        file_format='''(type = 'CSV', field_delimiter = ',', SKIP_HEADER = 1 \
            NULL_IF =('NULL','null',''), empty_field_as_null = true, FIELD_OPTIONALLY_ENCLOSED_BY = '\"' \
            ESCAPE_UNENCLOSED_FIELD = NONE RECORD_DELIMITER = '\n')''',
    )

    copy_into_prestg