from pyspark import pipelines as dp

CATALOG = spark.conf.get('catalog')
RUN_TESTS = spark.conf.get('run_tests', 'false') == 'true'

SILVER_ORDERS_CHECKS = {
    "silver_order_rows": "row_count = 3"
}

if RUN_TESTS:
    @dp.materialized_view(name = f'{CATALOG}.tests.orders_sdp_count')
    @dp.expect_all_or_fail(SILVER_ORDERS_CHECKS)
    def test_silver_orders_row_count_sdp():
        return spark.sql(f"""
           SELECT COUNT(*) as row_count FROM {CATALOG}.silver.orders_sdp              
        """)
    