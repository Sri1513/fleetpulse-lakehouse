from scripts.utils.dq_metrics import DataQualityMetrics

metrics = DataQualityMetrics("silver_equipment")

metrics.input_count = 10
metrics.after_dedup_count = 8
metrics.output_count = 7
metrics.null_primary_key_count = 0
metrics.duplicate_primary_key_count = 0

metrics.calculate_duplicates_removed()
metrics.calculate_invalid_records()

print(metrics.summary())