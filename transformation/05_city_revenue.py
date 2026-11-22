from pyspark.sql import functions as F
from pyspark import pipelines as dp
from src.retail.gold_aggregates import city_wise_revenue

CATALOG = spark.conf.get('catalog')
source_schema = 'silver'
source_table = 'orders_sdp'
target_schema = 'gold'

@dp.materialized_view(name=f'{CATALOG}.{target_schema}.city_revenue')
def calculate_monthly_sales():

    transaction_df = spark.read.table(f'{CATALOG}.{source_schema}.{source_table}')
    customer_df = spark.read.table(f'{CATALOG}.{source_schema}.customer_sdp')

    city_df = city_wise_revenue(transaction_df, customer_df)

    return city_df