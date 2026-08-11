class DataQualityMetrics:
    """
    Tracks data quality and processing metrics for one Silver table.
    
    """
    def __init__(self, table_name: str):

        self.table_name = table_name
        self.input_count = 0
        self.after_dedup_count = 0
        self.output_count = 0
        self.duplicates_removed = 0
        self.invalid_records = 0
        self.quarantined_records = 0
        self.null_primary_key_count = 0
        self.duplicate_primary_key_count = 0

    def calculate_duplicates_removed(self):
        self.duplicates_removed = self.input_count - self.after_dedup_count

    def calculate_invalid_records(self):
        self.invalid_records = self.after_dedup_count - self.output_count
        self.quarantined_records = self.invalid_records

    def pass_rate_pct(self):
        if self.input_count == 0:
            return 0.0
        return round((self.output_count / self.input_count) * 100, 2)

    def status(self):
        pass_rate = self.pass_rate_pct()

        if self.null_primary_key_count > 0:
            return "FAIL"
        if self.duplicate_primary_key_count > 0:
            return "FAIL"
        if pass_rate >= 95:
            return "PASS"
        if pass_rate >= 80:
            return "WARN"
        return "FAIL"
    def summary(self):
        return {
            "table": self.table_name,
            "input_rows": self.input_count,
            "after_dedup_rows": self.after_dedup_count,
            "output_rows": self.output_count,
            "duplicates_removed": self.duplicates_removed,
            "invalid_records": self.invalid_records,
            "quarantined_records": self.quarantined_records,
            "null_primary_key_count": self.null_primary_key_count,
            "duplicate_primary_key_count": self.duplicate_primary_key_count,
            "pass_rate_pct": self.pass_rate_pct(),
            "status": self.status()
        }