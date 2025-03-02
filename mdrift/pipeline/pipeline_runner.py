from typing import List

from pyspark.sql import DataFrame

from mdrift.checks.base_check import BaseQualityCheck
from mdrift.exception.custome_exception import DataQualityException
from mdrift.stages.data_reader import DataReader
from mdrift.stages.preprocessor import Preprocessor
from mdrift.stages.transformer import Transformer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PipelineRunner:
    def __init__(self, reader: DataReader, preprocessor: Preprocessor,
                 transformer: Transformer, quality_checks: List[BaseQualityCheck]):
        self.reader = reader
        self.preprocessor = preprocessor
        self.transformer = transformer
        self.quality_checks = quality_checks
        self.results = {}

    def execute_stage(self, df: DataFrame, stage_name: str,
                      quality_checks: List[BaseQualityCheck]) -> DataFrame:
        for check in quality_checks:
            df, check_results = check.execute(df)
            self.results[f"{stage_name}_{check.get_name()}"] = check_results
            # Check if rule is violated and handle accordingly
            if self._is_rule_violated(check, check_results):
                if check.is_hard_rule():
                    raise DataQualityException(
                        f"Hard rule {check.get_name()} failed at {stage_name}: {check_results}"
                    )
                else:
                    logger.warning(
                        f"Soft rule {check.get_name()} violated at {stage_name}: {check_results}"
                    )
        return df

    def _is_rule_violated(self, check: BaseQualityCheck, results: dict) -> bool:
        """Determine if a quality rule is violated based on results"""
        check_name = check.get_name()
        if check_name == "NullCheck":
            return any(results.get(f"{col}_null_count", 0) > 0 for col in check.columns)
        elif check_name == "DuplicateCheck":
            return results.get("duplicate_records", 0) > 0
        elif check_name == "RangeCheck":
            return results.get(f"{check.column}_out_of_range_count", 0) > 0
        return False

    def run(self, input_path: str) -> DataFrame:
        # Stage 1: Data Reading
        df = self.reader.read(input_path)
        df = self.execute_stage(df, "reading", self.quality_checks)

        # Stage 2: Preprocessing
        df = self.preprocessor.process(df)
        df = self.execute_stage(df, "preprocessing", self.quality_checks)

        # Stage 3: Transformation
        df = self.transformer.transform(df)
        df = self.execute_stage(df, "transformation", self.quality_checks)

        return df

    def get_quality_results(self) -> dict:
        return self.results