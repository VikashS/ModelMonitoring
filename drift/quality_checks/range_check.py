# quality_checks/range_check.py
from .base_check import DataQualityCheck
from pyspark.sql.functions import col

class RangeCheck(DataQualityCheck):
    def __init__(self, range_rules):
        self.range_rules = range_rules
        self.errors = []

    def check(self, df, stage="Unknown"):
        conditions = [
            (col(col_name) < min_val) | (col(col_name) > max_val)
            for col_name, (min_val, max_val) in self.range_rules.items()
        ]
        if conditions:
            combined_condition = conditions[0]
            for condition in conditions[1:]:
                combined_condition = combined_condition | condition
            issues = df.filter(combined_condition)
            issue_count = issues.count()
            if issue_count > 0:
                error_msg = f"Stage {stage}: {issue_count} rows out of range for {list(self.range_rules.keys())}"
                logging.error(error_msg)
                self.errors.append(error_msg)
            return (len(self.errors) == 0, issues)
        return (True, df.limit(0))

    def get_errors(self):
        return self.errors

    def reset_errors(self):
        self.errors = []