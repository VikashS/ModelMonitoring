# stages/preprocessing.py
import logging

class PreprocessingStage:
    def execute(self, df):
        """Simulate preprocessing (e.g., fill missing values)."""
        if df is None:
            return None
        logging.info("Executing Preprocessing Stage")
        return df.na.fill({"store_id": "UNKNOWN"})