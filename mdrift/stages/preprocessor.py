from pyspark.sql import DataFrame

class Preprocessor:
    def process(self, df: DataFrame) -> DataFrame:
        # Add preprocessing logic here
        return df.na.fill(0)  # Example: fill nulls with 0