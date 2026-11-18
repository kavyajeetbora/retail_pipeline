from pyspark import pipelines as dp
from pyspark.sql import functions as F

source_path = spark.conf.get('bronze.source_path')

sources = {
    'customer_sdp': f'{source_path}/customers',
    'orders_sdp': f'{source_path}/transaction'
}

def create_bronze_table(table_name, source_path):
    @dp.table(name=table_name, table_properties = {'quality':'bronze'})
    def bronze_customers():
        return (
            spark.readStream.format('cloudFiles')
            .option('cloudFiles.format', 'csv')
            .option('header', 'true')
            .load(source_path)
            .select(
                '*',
                F.current_timestamp().alias('_bronze_ingested_at'),
                F.col('_metadata.file_path').alias('_bronze_file_name'),
                F.col('_metadata.file_modification_time').alias('_file_modification_time')
            )
        )

for table_name, source_path in sources.items():
    create_bronze_table(table_name, source_path)