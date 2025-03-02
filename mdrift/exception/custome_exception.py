class DataQualityException(Exception):
    """Custom exception raised when a hard data quality rule is violated at runtime."""

    def __init__(
        self,
        message: str,
        rule_name: str = None,
        stage: str = None,
        results: dict = None,
    ):
        """
        Initialize the exception with details about the violation.

        Args:
            message (str): The error message.
            rule_name (str, optional): Name of the rule that failed.
            stage (str, optional): Stage where the violation occurred.
            results (dict, optional): Quality check results that caused the failure.
        """
        self.rule_name = rule_name
        self.stage = stage
        self.results = results or {}

        # Construct detailed message
        full_message = message
        if rule_name and stage:
            full_message = (
                f"Hard rule '{rule_name}' failed at stage '{stage}': {message}"
            )
        if results:
            full_message += f" (Results: {results})"

        super().__init__(full_message)

    def __str__(self):
        """String representation of the exception."""
        return super().__str__()
