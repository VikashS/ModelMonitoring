# quality_checks/consistency.py
from .base_check import DataQualityCheck
from pyspark.sql.functions import col, abs

class ConsistencyCheck(DataQualityCheck):
    def __init__(self, calculated_col, expected_expr, tolerance=0.01):
        self.calculated_col = calculated_col
        self.expected_expr = expected_expr
        self.tolerance = tolerance
        self.errors = []

    def check(self, df, stage="Unknown"):
        df_with_expected = df.withColumn("expected_value", col(self.expected_expr))
        issues = df_with_expected.filter(
            abs(col(self.calculated_col) - col("expected_value")) > self.tolerance
        )
        issue_count = issues.count()
        if issue_count > 0:
            error_msg = f"Stage {stage}: {issue_count} rows inconsistent in '{self.calculated_col}'"
            logging.error(error_msg)
            self.errors.append(error_msg)
        return (len(self.errors) == 0, issues.drop("expected_value"))

    def get_errors(self):
        return self.errors

    def reset_errors(self):
        self.errors = []