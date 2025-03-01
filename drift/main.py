# main.py
from config.config import QUALITY_CHECK_CONFIG
from stages.data_reading import DataReadingStage
from stages.preprocessing import PreprocessingStage
from stages.transformation import DataTransformationStage
from quality_checks.completeness import CompletenessCheck
from quality_checks.consistency import ConsistencyCheck
from quality_checks.range_check import RangeCheck
from pipeline.runner import PipelineRunner
from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("ETL_Pipeline").getOrCreate()

    # Initialize quality checks
    completeness = CompletenessCheck(QUALITY_CHECK_CONFIG["completeness"]["critical_columns"])
    consistency = ConsistencyCheck(
        QUALITY_CHECK_CONFIG["consistency"]["calculated_col"],
        QUALITY_CHECK_CONFIG["consistency"]["expected_expr"],
        QUALITY_CHECK_CONFIG["consistency"]["tolerance"]
    )
    range_check = RangeCheck(QUALITY_CHECK_CONFIG["range"]["range_rules"])

    # Define ETL stages
    stages = [
        DataReadingStage(),
        PreprocessingStage(),
        DataTransformationStage()
    ]

    # Create and run pipeline
    pipeline = PipelineRunner(stages=stages, quality_checks=[completeness, consistency, range_check])
    result_df = pipeline.run()

    if result_df is not None:
        print("Final DataFrame:")
        result_df.show(truncate=False)

    # Stop Spark session
    spark.stop()