# config/config.py
QUALITY_CHECK_CONFIG = {
    "completeness": {
        "critical_columns": ["transaction_id", "store_id"]
    },
    "consistency": {
        "calculated_col": "total_amount",
        "expected_expr": "quantity * unit_price",
        "tolerance": 0.01
    },
    "range": {
        "range_rules": {
            "quantity": (0, 100),
            "unit_price": (0, 1000)
        }
    }
}