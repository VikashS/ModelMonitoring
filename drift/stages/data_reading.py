# stages/data_reading.py
import logging
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ETL_DataReading").getOrCreate()

class DataReadingStage:
    def execute(self):
        """Simulate data reading."""
        logging.info("Executing Data Reading Stage")
        data = [
            ("TXN001", "STR001", 2, 50.00, 100.00),
            ("TXN002", "STR002", 3, 20.00, 60.00),
            ("TXN003", None, 1, 150.00, 149.99),  # Missing store_id
            ("TXN004", "STR003", -1, 50.00, 50.00)  # Negative quantity
        ]
        columns = ["transaction_id", "store_id", "quantity", "unit_price", "total_amount"]
        return spark.createDataFrame(data, columns)