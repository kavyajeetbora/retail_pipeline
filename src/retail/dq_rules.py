ORDER_DROP_RULES = {
    "valid_orders_id": "txn_id IS NOT NULL AND txn_id RLIKE '^T[0-9]{5}$'",
    "valid_transaction_timestamp": "txn_ts IS NOT NULL",
    "valid_amount": "amount IS NOT NULL AND amount > 0"
}

ORDER_WARN_RULES = {
    "valid_customer_id": "customer_id IS NOT NULL"
}

CUSTOMER_DROP_RULES = {
    "valid_customer_id": "customer_id IS NOT NULL",
    'valid_updated_at': "updated_at IS NOT NULL"
}

CUSTOMER_WARN_RULES = {
    'valid_segment': 'segment is NOT NULL'
}