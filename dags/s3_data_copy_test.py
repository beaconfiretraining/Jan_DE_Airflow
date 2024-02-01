"""
Example use of Snowflake related operators.
"""
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
#S3_FILE_PATH = 'product_order_trans_07152022.csv'

with DAG(
<<<<<<< Updated upstream
    "s3_data_copy_test",
    start_date=datetime(2022, 11, 28),
    schedule_interval='0 7 * * *',
=======
    "s3_data_copy_test_group3",
    start_date=datetime(2024, 1, 30),
    schedule_interval='0 0 * * *',
>>>>>>> Stashed changes
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=True,
) as dag:

    copy_into_prestg = S3ToSnowflakeOperator(
<<<<<<< Updated upstream
        task_id='prestg_product_order_trans',
        s3_keys=['product_order_trans_{{ ds[5:7]+ds[8:10]+ds[0:4] }}.csv'],
        table='prestg_product_order_trans',
=======
        task_id='prestg_product_info_group3',
        s3_keys=['product_info_Group3_{{ ds[5:7]+ds[8:10]+ds[0:4] }}.csv'], #YYYY-MM-DD
        table='PRESTAGE_PRODUCT_INFO_GROUP3',
>>>>>>> Stashed changes
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        file_format='''(type = 'CSV', field_delimiter = ',', SKIP_HEADER = 1 \
            NULL_IF =('NULL','null',''), empty_field_as_null = true, FIELD_OPTIONALLY_ENCLOSED_BY = '\"' \
            ESCAPE_UNENCLOSED_FIELD = NONE RECORD_DELIMITER = '\n')''',
    )

    copy_into_prestg
          
        
    

