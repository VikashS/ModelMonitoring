from pyspark.sql import SparkSession, DataFrame


class DataReader:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read(self, path: str, format: str = "csv") -> DataFrame:
        return self.spark.read.format(format).option("header", "true").load(path)
