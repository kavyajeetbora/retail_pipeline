from pyspark.sql import functions as F
from pyspark import pipelines as dp
from src.retail.transformation import clean_customers
from src.retail.dq_rules import ORDER_DROP_RULES, ORDER_WARN_RULES, CUSTOMER_DROP_RULES, CUSTOMER_WARN_RULES

CATALOG = spark.conf.get('catalog')
SOURCE_SCHEMA = 'bronze'
TARGET_SCHEMA = 'silver'
TABLE_NAME = 'customer_sdp'

@dp.temporary_view(name='customer_clean')
@dp.expect_all_or_drop(CUSTOMER_DROP_RULES)
@dp.expect_all(CUSTOMER_WARN_RULES)
def silver_customers():
    return (
        clean_customers(spark.readStream.table(f'{CATALOG}.{SOURCE_SCHEMA}.{TABLE_NAME}'))
    )

## Create the empty table in silver
target_table_name = f'{CATALOG}.{TARGET_SCHEMA}.{TABLE_NAME}'
dp.create_streaming_table(
    name = target_table_name, 
    table_properties = {
        'quality': 'silver',
        'delta.enableChangeDataFeed': 'true'
    }
)

## Ingest the data from bronze
dp.create_auto_cdc_flow(
    target = target_table_name,
    source = 'customer_clean',
    keys = ['customer_id'],
    sequence_by = F.struct('updated_at', '_file_modification_time'),
    track_history_column_list=['first_name', 'last_name', 'email', 'phone', 'city', 'segment'],
    stored_as_scd_type=2
)
