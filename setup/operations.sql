SELECT
  ROUND(SUM(u.usage_quantity), 2)                      AS dbus,
  ROUND(SUM(u.usage_quantity * p.pricing.default), 2)  AS cost_usd
FROM system.billing.usage u
JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
 AND p.price_end_time IS NULL
WHERE u.usage_metadata.job_id = '477674165220768';

-- 1. How fresh is the billing data?
SELECT MAX(usage_end_time) AS latest_billing_record
FROM system.billing.usage;

-- 2. When did the job actually run?
SELECT period_start_time, period_end_time, result_state
FROM system.lakeflow.job_run_timeline
WHERE job_id = '477674165220768'
ORDER BY period_start_time DESC
LIMIT 10;

SELECT MAX(usage_end_time) AS latest_billing_record
FROM system.billing.usage;