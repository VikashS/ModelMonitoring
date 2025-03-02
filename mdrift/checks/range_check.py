from pyspark.sql import DataFrame

from mdrift.checks.base_check import BaseQualityCheck


class RangeCheck(BaseQualityCheck):
    def __init__(self, column: str, min_val: float, max_val: float,severity:str="soft"):
        super().__init__(severity)
        self.column = column
        self.min_val = min_val
        self.max_val = max_val

    def execute(self, df: DataFrame) -> tuple[DataFrame, dict]:
        out_of_range_count = df.filter(
            (df[self.column] < self.min_val) |
            (df[self.column] > self.max_val)
        ).count()

        total_count = df.count()
        results = {
            f"{self.column}_out_of_range_count": out_of_range_count,
            f"{self.column}_out_of_range_percentage": (out_of_range_count / total_count) * 100
        }
        return df, results

    def get_name(self) -> str:
        return "RangeCheck"