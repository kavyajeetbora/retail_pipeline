from pyspark.sql import functions as F
from pyspark import pipelines as dp


CATALOG = spark.conf.get('catalog', 'retail')
SOURCE_SCHEMA = 'bronze'
TARGET_SCHEMA = 'silver'
TABLE_NAME = 'customer_sdp'


@dp.temporary_view(name='customer_clean')
@dp.expect_or_drop('valid_customer_id', "customer_id IS NOT NULL and customer_id <> ''")
@dp.expect_or_drop('valid_updated_at', "updated_at IS NOT NULL")
@dp.expect('valid_segment', 'segment is NOT NULL')
def silver_customers():
    return (
        spark.readStream.table(f'{CATALOG}.{SOURCE_SCHEMA}.{TABLE_NAME}')
        .withColumn('first_name', F.initcap(F.trim('first_name')))
        .withColumn('last_name',  F.initcap(F.trim('last_name')))
        .withColumn('email', F.lower(F.trim('email')))
        .withColumn('city', F.initcap(F.trim('city')))
        .withColumn('segment', F.initcap(F.trim('segment')))
        .withColumn('updated_at', F.try_to_timestamp('updated_at', F.lit('yyyy-MM-dd HH:mm:ss')))
        .withColumn('_silver_updated_at', F.current_timestamp())
    )

## Create the empty table in silver
target_table_name = f'{CATALOG}.{TARGET_SCHEMA}.{TABLE_NAME}'
dp.create_streaming_table(target_table_name)

## Ingest the data from bronze
dp.create_auto_cdc_flow(
    target = target_table_name,
    source = 'customer_clean',
    keys = ['customer_id'],
    sequence_by = F.struct('updated_at', '_file_modification_time'),
    track_history_column_list=['first_name', 'last_name', 'email', 'phone', 'city', 'segment'],
    stored_as_scd_type=2
)
