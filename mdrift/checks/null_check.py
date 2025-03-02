from pyspark.sql import DataFrame

from mdrift.checks.base_check import BaseQualityCheck


class NullCheck(BaseQualityCheck):
    def __init__(self, columns: list, severity: str = "soft"):
        super().__init__(severity)
        self.columns = columns

    def execute(self, df: DataFrame) -> tuple[DataFrame, dict]:
        results = {}
        for col in self.columns:
            null_count = df.filter(df[col].isNull()).count()
            results[f"{col}_null_count"] = null_count
            results[f"{col}_null_percentage"] = (null_count / df.count()) * 100
        return df, results

    def get_name(self) -> str:
        return "NullCheck"
