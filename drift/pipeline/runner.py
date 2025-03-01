# pipeline/runner.py
import logging


class PipelineRunner:
    def __init__(self, stages, quality_checks):
        self.stages = stages
        self.quality_checks = quality_checks

    def run_stage(self, stage, df, stage_name):
        """Run a single stage and apply quality checks."""
        result_df = stage.execute(df)
        if result_df is None:
            logging.error(f"{stage_name} failed: No data to process.")
            return None

        for check in self.quality_checks:
            check.reset_errors()

        all_checks_passed = True
        for check in self.quality_checks:
            passed, issues_df = check.check(result_df, stage=stage_name)
            if not passed:
                all_checks_passed = False
                # Uncomment to inspect issues
                # issues_df.show()

        if not all_checks_passed:
            logging.error(f"{stage_name} failed due to quality issues.")
            return None

        return result_df

    def run(self):
        """Execute the pipeline sequentially."""
        df = None
        stage_names = ["Data Reading", "Preprocessing", "Data Transformation"]

        for stage, stage_name in zip(self.stages, stage_names):
            df = self.run_stage(stage, df, stage_name)
            if df is None:
                logging.error("Pipeline aborted.")
                return None

        logging.info("Pipeline completed successfully.")
        return df