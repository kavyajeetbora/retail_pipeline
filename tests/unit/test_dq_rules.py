from retail.dq_rules import ORDER_DROP_RULES
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import pytest

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()

def test_valid_amount_keeps_only_positive_amounts(spark):
    
    df = spark.createDataFrame(
        [(100.0,), (0.0,), (-50.0,), (None,)],
        "amount double",
    )

    expected_df = spark.createDataFrame(
        [(100.0,)],
        "amount double",
    )

    actual_df = df.filter(F.expr(f"{ORDER_DROP_RULES['valid_amount']}"))

    assertDataFrameEqual(actual_df, expected_df)

def test_valid_transaction_id(spark):

    data = ['T001', 'T123451', 'T12345',  'C0001', 'casas', 'xxaaa', '123', None, '', 'T00180']
    df = spark.createDataFrame(data, ['txn_id'])

    expected_df = spark.createDataFrame(['T12345',  'T00180'], ['txn_id'])

    transformed_df = df.filter(F.expr(f"{ORDER_DROP_RULES['valid_orders_id']}"))

    assertDataFrameEqual(transformed_df, expected_df)


## Gold Table Check
def total_sales(saprk):

    

