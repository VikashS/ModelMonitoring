# quality_checks/completeness.py
from .base_check import DataQualityCheck
from pyspark.sql.functions import col

class CompletenessCheck(DataQualityCheck):
    def __init__(self, critical_columns):
        self.critical_columns = critical_columns
        self.errors = []

    def check(self, df, stage="Unknown"):
        conditions = [col(c).isNull() for c in self.critical_columns]
        combined_condition = conditions[0]
        for condition in conditions[1:]:
            combined_condition = combined_condition | condition
        issues = df.filter(combined_condition)
        issue_count = issues.count()
        if issue_count > 0:
            error_msg = f"Stage {stage}: {issue_count} rows with missing values in {self.critical_columns}"
            logging.error(error_msg)
            self.errors.append(error_msg)
        return (len(self.errors) == 0, issues)

    def get_errors(self):
        return self.errors

    def reset_errors(self):
        self.errors = []