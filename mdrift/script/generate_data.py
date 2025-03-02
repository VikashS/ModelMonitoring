from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, expr
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)
import random
from datetime import datetime, timedelta
import os


def generate_timestamp(start_date: datetime, days_range: int) -> str:
    """Generate a random timestamp within a range."""
    random_days = random.randint(0, days_range)
    random_seconds = random.randint(0, 86399)  # Seconds in a day
    new_date = start_date + timedelta(days=random_days, seconds=random_seconds)
    return new_date.strftime("%Y-%m-%d %H:%M:%S")


def create_passing_data(
    spark: SparkSession, output_path: str, num_rows: int = 1000
) -> None:
    """Generate data that passes all quality checks."""
    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("timestamp", StringType(), False),
            StructField("value", DoubleType(), False),
        ]
    )

    start_date = datetime(2023, 1, 1)

    # Generate data
    data = [
        (
            i,
            generate_timestamp(start_date, 365),
            random.uniform(0.0, 4166.66),  # Max value * 1.2 <= 5000
        )
        for i in range(num_rows)
    ]

    df = spark.createDataFrame(data, schema)
    df.write.mode("overwrite").parquet(output_path)
    print(f"Generated passing data with {num_rows} rows at {output_path}")


def create_failing_data(
    spark: SparkSession, output_path: str, num_rows: int = 1000
) -> None:
    """Generate data that fails some quality checks."""
    schema = StructType(
        [
            StructField("id", IntegerType(), True),  # Allow nulls
            StructField("timestamp", StringType(), True),  # Allow nulls
            StructField("value", DoubleType(), True),  # Allow nulls
        ]
    )

    start_date = datetime(2023, 1, 1)

    # Generate data with issues
    data = []
    for i in range(num_rows):
        id_val = i if random.random() > 0.1 else None  # 10% null IDs
        ts_val = (
            generate_timestamp(start_date, 365)
            if random.random() > 0.05
            else "2023-13-45"  # 5% invalid timestamps
        )
        val = (
            random.uniform(0.0, 6000.0)
            if random.random() > 0.15  # Some values > 4166.66
            else None  # 15% null values
        )
        data.append((id_val, ts_val, val))

    df = spark.createDataFrame(data, schema)
    df.write.mode("overwrite").parquet(output_path)
    print(f"Generated failing data with {num_rows} rows at {output_path}")


def main():
    spark = SparkSession.builder.appName("GenerateDummyData").getOrCreate()

    # Ensure data directory exists
    os.makedirs("data", exist_ok=True)

    # Generate datasets
    create_passing_data(spark, "data/passing_data.parquet", 1000)
    create_failing_data(spark, "data/failing_data.parquet", 1000)

    spark.stop()


if __name__ == "__main__":
    main()
