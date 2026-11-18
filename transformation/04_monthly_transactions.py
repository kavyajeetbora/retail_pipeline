from pyspark.sql import functions as F
from pyspark import pipelines as dp

CATALOG = spark.conf.get('catalog', 'retail')
source_schema = 'silver'
source_table = 'orders_sdp'
target_schema = 'gold'

@dp.materialized_view(name=f'{CATALOG}.{target_schema}.monthly_sales')
def calculate_monthly_sales():
    return (
        spark.read.table(f'{CATALOG}.{source_schema}.{source_table}')
        .withColumn('month', F.month('txn_ts').alias('month'))
        .groupBy('month').agg(
            F.sum('amount').alias('total_sales')
        )
        .select(
            F.col('month'),
            F.col('total_sales')
        )
    )

