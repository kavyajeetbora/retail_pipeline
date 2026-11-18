from pyspark.sql import functions as F
from pyspark import pipelines as dp

CATALOG = spark.conf.get('catalog', 'retail')
source_schema = 'silver'
source_table = 'orders_sdp'
target_schema = 'gold'

@dp.materialized_view(name=f'{CATALOG}.{target_schema}.city_revenue')
def calculate_monthly_sales():
    return (
        spark.read.table(f'{CATALOG}.{source_schema}.{source_table}').alias('t')
        .join(
            spark.read.table(f'{CATALOG}.{source_schema}.customer_sdp').alias('c'),
            F.col('t.customer_id') == F.col("c.customer_id"),
            'inner'
        )
        .groupBy('c.city').agg(
            F.sum('t.amount').alias('total_sales')
        )
        .select(
            F.col('c.city').alias('city'),
            F.col('total_sales')
        )
    )

