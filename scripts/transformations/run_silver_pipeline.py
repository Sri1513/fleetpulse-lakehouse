from scripts.utils.spark_session import create_spark_session
from scripts.utils.config import(
    BRONZE_BASE_PATH,
    SILVER_BASE_PATH
)
from scripts.utils.dq_metrics import DataQualityMetrics
from scripts.config.silver_table_config import SILVER_TABLE_CONFIG
from scripts.utils.silver_utils import(
    deduplicate_latest,
    standardize_columns,
    apply_defaults,
    add_record_hash,
    add_silver_audit_columns,
    validate_primary_key,
    split_valid_invalid_records,
    apply_referential_checks
)

from pathlib import Path

def process_table(spark, table_config):
    bronze_path = f"{BRONZE_BASE_PATH}/{table_config['table_name']}"
    silver_path = f"{SILVER_BASE_PATH}/{table_config['table_name']}"
    metrics = DataQualityMetrics(table_name=table_config['table_name'])
    bronze_df = spark.read.parquet(bronze_path)
    metrics.input_count = bronze_df.count()
    stand_df = standardize_columns(df=bronze_df,trim_columns=table_config['trim_columns'],uppercase_columns=table_config['uppercase_columns'])
    defualt_df = apply_defaults(df=stand_df,defaults=table_config['defaults'])
    dedup_df = deduplicate_latest(df=defualt_df,key_columns=table_config['primary_key'],order_columns=table_config['order_columns'])
    metrics.after_dedup_count = dedup_df.count()
    metrics.calculate_duplicates_removed()
    hash_df = add_record_hash(df=dedup_df,hash_columns=table_config['hash_columns'])
    result = split_valid_invalid_records(hash_df,required_columns=table_config['required_columns'])
    valid_raw_df = result['valid']
    invalid_raw_df = result['invalid']
    if "referential_checks" in table_config:
        valid_raw_df = apply_referential_checks(
            df=valid_raw_df,
            spark=spark,
            referential_checks=table_config['referential_checks'],
            silver_base_path=SILVER_BASE_PATH
            )
    valid_df = add_silver_audit_columns(valid_raw_df, quality_status="VALID")
    invalid_df = add_silver_audit_columns(invalid_raw_df, quality_status="INVALID")
    
    validation_results = validate_primary_key(df=valid_df,primary_key=table_config['primary_key'][0])
    metrics.null_primary_key_count = validation_results['null_primary_key_count']
    metrics.duplicate_primary_key_count = validation_results['dupe_primary_key_count']
    valid_df.write.mode("overwrite").parquet(silver_path)
    metrics.output_count = valid_df.count()
    metrics.calculate_invalid_records()

    return metrics.summary()


def main():
    spark = None
    tables_to_process = [
    "branches",
    "customers",
    "equipment_categories",
    "equipment",
    "rental_contracts",
    "rental_contract_lines",
    "invoices",
    "invoice_lines",
    "payments",
    "maintenance_work_orders",
    "equipment_status_history"
    ]
    table_config = {}
    all_results = []
    spark = create_spark_session("LoadingBranchToSilver")
    try:
        for table in tables_to_process:
            table_config = SILVER_TABLE_CONFIG[table]
            result = process_table(spark,table_config)
            all_results.append(result)
            print(result)
        print("=" * 80)
        print("SILVER PIPELINE SUMMARY")
        print("=" * 80)
        for result in all_results:
            print(
                    f"[{result['status']}] {result['table']}: "
                    f"input={result['input_rows']}, "
                    f"output={result['output_rows']}, "
                    f"dedup={result['duplicates_removed']}, "
                    f"quarantined={result['quarantined_records']}, "
                    f"pass_rate={result['pass_rate_pct']}%"
                    )
    finally :
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()
