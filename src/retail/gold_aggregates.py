from pyspark.sql import functions as F
from pyspark.sql import DataFrame

def city_wise_revenue(transaction_df: DataFrame, customer_df: DataFrame) -> DataFrame:

    current_customer_df = customer_df.filter(F.col('__END_AT').isNull()).select('customer_id', 'city')
    
    return(
        transaction_df.alias('t')
        .join(
            current_customer_df.alias('c'),
            F.col('t.customer_id') == F.col("c.customer_id"),
            'left'
        )
        .withColumn('city', F.coalesce (F.col('c.city'), F.lit('[UNASSINGED]')))
        .groupBy('city')
        .agg(F.sum('t.amount').alias('total_sales'))
    )
    