from pyspark.sql import functions as F
from pyspark import pipelines as dp

CATALOG = spark.conf.get('catalog', 'retail')
SOURCE_SCHEMA = 'bronze'
TARGET_SCHEMA = 'silver'
TABLE_NAME = 'orders_sdp'

SOURCE_TABLE_NAME = f"{CATALOG}.{SOURCE_SCHEMA}.{TABLE_NAME}"
TARGET_TABLE_NAME = f"{CATALOG}.{TARGET_SCHEMA}.{TABLE_NAME}"

@dp.temporary_view(name='orders_transformed')
@dp.expect_or_drop('valid_orders_id', "txn_id IS NOT NULL OR txn_id <> ''")
@dp.expect_or_drop('valid_customer_id', "customer_id IS NOT NULL")
@dp.expect_or_drop('valid_transaction_timestamp', 'txn_ts IS NOT NULL')
@dp.expect_or_drop('valid_amount', "amount IS NOT NULL OR amount > 0")
def orders_transform():
    return (
        spark.readStream.table(SOURCE_TABLE_NAME)
        .withColumn('txn_id', F.trim('txn_id'))
            .withColumn('txn_id', F.trim('txn_id'))
            .withColumn('customer_id', F.trim('customer_id'))
            .withColumn('txn_ts', F.try_to_timestamp(F.trim('txn_ts'), F.lit('yyyy-MM-dd HH:mm:ss')))
            .withColumn('amount', F.round('amount', 2))
            .withColumn('_silver_updated_at', F.current_timestamp())   
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

 
