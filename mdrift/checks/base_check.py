from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class BaseQualityCheck(ABC):

    def __init__(self,severity:str="soft"):
        self.severity=severity.lower()

    @abstractmethod
    def execute(self, df: DataFrame) -> tuple[DataFrame, dict]:
        """Execute the quality check and return DataFrame and results"""
        pass

    @abstractmethod
    def get_name(self) -> str:
        """Return the name of the quality check"""
        pass

    def is_hard_rule(self)-> bool:
        return self.severity == "hard"