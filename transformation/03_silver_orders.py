from pyspark.sql import functions as F
from pyspark import pipelines as dp
from src.retail.dq_rules import ORDER_DROP_RULES, ORDER_WARN_RULES, CUSTOMER_DROP_RULES, CUSTOMER_WARN_RULES
from src.retail.transformation import clean_orders

CATALOG = spark.conf.get('catalog')
SOURCE_SCHEMA = 'bronze'
TARGET_SCHEMA = 'silver'
TABLE_NAME = 'orders_sdp'

SOURCE_TABLE_NAME = f"{CATALOG}.{SOURCE_SCHEMA}.{TABLE_NAME}"
TARGET_TABLE_NAME = f"{CATALOG}.{TARGET_SCHEMA}.{TABLE_NAME}"

@dp.temporary_view(name='orders_transformed')
@dp.expect_all_or_drop(ORDER_DROP_RULES)
@dp.expect_all(ORDER_WARN_RULES)
def orders_transform():
    return (
        clean_orders(spark.readStream.table(SOURCE_TABLE_NAME))
    )


## 2. Create the silver table
dp.create_streaming_table(
    name = TARGET_TABLE_NAME,
    table_properties = {
        'quality': 'silver',
        'delta.enableChangeDataFeed': 'true'
    }    
)

## Incrementally ingest the data from bronze to the table
dp.create_auto_cdc_flow(
    target = TARGET_TABLE_NAME,
    source = 'orders_transformed',
    keys = ['txn_id'],
    sequence_by = F.struct('_bronze_ingested_at', '_file_modification_time'), 
    stored_as_scd_type=1
)

 
