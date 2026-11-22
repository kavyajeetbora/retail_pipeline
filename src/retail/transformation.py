from pyspark.sql import DataFrame, functions as F

def clean_orders(df: DataFrame) -> DataFrame:
  return (
      df
        .withColumn('txn_id', F.trim('txn_id'))
        .withColumn('customer_id', F.trim('customer_id'))
        .withColumn('txn_ts', F.try_to_timestamp(F.trim('txn_ts'), F.lit('yyyy-MM-dd HH:mm:ss')))
        .withColumn('amount', F.col('amount').cast("double"))
        .withColumn('_silver_updated_at', F.current_timestamp())
  )

def clean_customers(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn('first_name', F.initcap(F.trim('first_name')))
        .withColumn('last_name',  F.initcap(F.trim('last_name')))
        .withColumn('email', F.lower(F.trim('email')))
        .withColumn('city', F.initcap(F.trim('city')))
        .withColumn('segment', F.initcap(F.trim('segment')))
        .withColumn('updated_at', F.try_to_timestamp('updated_at', F.lit('yyyy-MM-dd HH:mm:ss')))
        .withColumn('_silver_updated_at', F.current_timestamp())
    )

