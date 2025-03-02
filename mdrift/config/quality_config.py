QUALITY_CHECKS = {
    "null_check": {
        "columns": ["id", "value"],
        "severity": "soft",  # Hard rule: Nulls in critical columns stop the job
    },
    "duplicate_check": {
        "columns": ["id"],
        "severity": "soft",  # Soft rule: Duplicates trigger a warning but job continues
    },
    "range_check": {
        "column": "value",
        "min": 0,
        "max": 100,
        "severity": "soft",  # Soft rule: Out-of-range values trigger a warning
    },
}
