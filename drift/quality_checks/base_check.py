# quality_checks/base_check.py
from abc import ABC, abstractmethod
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataQualityCheck(ABC):
    @abstractmethod
    def check(self, df, stage="Unknown"):
        """
        Perform a data quality check.
        :param df: PySpark DataFrame
        :param stage: ETL stage name
        :return: Tuple (Boolean, PySpark DataFrame with issues)
        """
        pass

    @abstractmethod
    def get_errors(self):
        """Return recorded errors."""
        pass

    def reset_errors(self):
        """Reset errors for a fresh check."""
        pass