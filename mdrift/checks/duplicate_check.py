from pyspark.sql import DataFrame

from mdrift.checks.base_check import BaseQualityCheck


class DuplicateCheck(BaseQualityCheck):
    def __init__(self, key_columns: list,severity:str="soft"):
        super().__init__(severity)
        self.key_columns = key_columns

    def execute(self, df: DataFrame) -> tuple[DataFrame, dict]:
        total_count = df.count()
        distinct_count = df.dropDuplicates(self.key_columns).count()
        duplicate_count = total_count - distinct_count

        results = {
            "total_records": total_count,
            "duplicate_records": duplicate_count,
            "duplicate_percentage": (duplicate_count / total_count) * 100
        }
        return df, results

    def get_name(self) -> str:
        return "DuplicateCheck"