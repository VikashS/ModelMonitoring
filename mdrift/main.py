from pyspark.sql import SparkSession

from mdrift.checks.duplicate_check import DuplicateCheck
from mdrift.checks.null_check import NullCheck
from mdrift.checks.range_check import RangeCheck
from mdrift.config.quality_config import QUALITY_CHECKS
from mdrift.pipeline.pipeline_runner import PipelineRunner
from mdrift.stages.data_reader import DataReader
from mdrift.stages.preprocessor import Preprocessor
from mdrift.stages.transformer import Transformer


def main():
    # Initialize Spark
    spark = SparkSession.builder.appName("DataQualityPipeline").getOrCreate()

    # Initialize components
    reader = DataReader(spark)
    preprocessor = Preprocessor()
    transformer = Transformer()

    # Initialize quality checks
    # Initialize quality checks with severity from config
    quality_checks = [
        NullCheck(
            columns=QUALITY_CHECKS["null_check"]["columns"],
            severity=QUALITY_CHECKS["null_check"]["severity"]
        ),
        DuplicateCheck(
            key_columns=QUALITY_CHECKS["duplicate_check"]["columns"],
            severity=QUALITY_CHECKS["duplicate_check"]["severity"]
        ),
        RangeCheck(
            column=QUALITY_CHECKS["range_check"]["column"],
            min_val=QUALITY_CHECKS["range_check"]["min"],
            max_val=QUALITY_CHECKS["range_check"]["max"],
            severity=QUALITY_CHECKS["range_check"]["severity"]
        )
    ]

    # Create and run pipeline
    pipeline = PipelineRunner(reader, preprocessor, transformer, quality_checks)
    result_df = pipeline.run("data.csv")

    # Print quality results
    quality_results = pipeline.get_quality_results()
    for stage, results in quality_results.items():
        print(f"\n{stage} Results:")
        for key, value in results.items():
            print(f"{key}: {value}")

    # Show final DataFrame
    result_df.show()

    spark.stop()


if __name__ == "__main__":
    main()