from retail.gold_aggregates import city_wise_revenue
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import pytest

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()


def test_city_revenue_aggregates(spark):

    ## Sample Transaction Table
    transactions = spark.createDataFrame(
        [
            ("T001", "C01", 100.0),   # C01 -> current city
            ("T002", "C01", 50.0),    # C01 again -> same city, adds up
            ("T003", "C02", 200.0),   # C02 -> only one version
            ("T004", "C99", 30.0),    # orphan: C99 not in customers
            ("T005", None,  20.0),    # no customer at all
        ],
        "txn_id string, customer_id string, amount double",
    )

    ## Sample Customer Table
    customers = (
        spark.createDataFrame(
            [
                ("C01", "Delhi",  "2026-11-01 00:00:00", "2026-11-03 00:00:00"),  # old version
                ("C01", "Pune",   "2026-11-03 00:00:00", None),                   # current
                ("C02", "Mumbai", "2026-11-01 00:00:00", None),                   # current
                ("C04", "Mumbai",   "2026-11-03 00:00:00", None),                   # current
                ("C05", "Pune",   "2026-11-03 00:00:00", None),                   # current
                ("C99", "Delhi",   "2026-11-03 00:00:00", None),                   # current

            ],
            "customer_id string, city string, __START_AT string, __END_AT string",
        )
        .withColumn("__START_AT", F.to_timestamp("__START_AT"))
        .withColumn("__END_AT", F.to_timestamp("__END_AT"))
    )

    expected_city_revenue_df = (
        spark.createDataFrame(
            [
                ("Pune", 150),
                ("Mumbai", 200),
                ("Delhi", 30),
                ("[UNASSINGED]", 20)
            ],
            "city string, total_sales double",
        )
    )

    city_revenue_df = city_wise_revenue(transactions, customers)

    assertDataFrameEqual(expected_city_revenue_df, city_revenue_df)





