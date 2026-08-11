from scripts.utils.spark_session import create_spark_session
from scripts.utils.silver_utils import (
    deduplicate_latest,
    standardize_columns,
    apply_defaults,
    add_record_hash,
    add_silver_audit_columns,
    validate_primary_key,
    split_valid_invalid_records
)

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, BooleanType
from datetime import datetime


def main():
    spark = None

    try:
        spark = create_spark_session("Test Silver Utils")

        test_data = [
            (1, " cust-001 ", "john smith", " active ", "good", datetime(2026, 8, 1, 10, 0, 0), False),
            (1, " CUST-001 ", "John Smith Updated", "ACTIVE", "GOOD", datetime(2026, 8, 2, 10, 0, 0), False),
            (2, "cust-002", "jane doe", None, "hold", datetime(2026, 8, 1, 11, 0, 0), None),
            (3, None, "missing customer number", "ACTIVE", "review", datetime(2026, 8, 1, 12, 0, 0), False),
            (None, "cust-004", "missing customer id", "ACTIVE", "good", datetime(2026, 8, 1, 13, 0, 0), False),
        ]

        schema = StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("customer_number", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("customer_status", StringType(), True),
            StructField("credit_status", StringType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("is_deleted", BooleanType(), True),
        ])

        df = spark.createDataFrame(test_data, schema)

        print("=" * 80)
        print("Original Data")
        df.show(truncate=False)

        df_standardized = standardize_columns(
            df=df,
            trim_columns=["customer_name"],
            uppercase_columns=["customer_number", "customer_status", "credit_status"]
        )

        df_defaulted = apply_defaults(
            df=df_standardized,
            defaults={
                "customer_status": "UNKNOWN",
                "credit_status": "UNKNOWN",
                "is_deleted": False
            }
        )

        df_deduped = deduplicate_latest(
            df=df_defaulted,
            key_columns=["customer_id"],
            order_columns=["updated_at"]
        )

        df_hashed = add_record_hash(
            df=df_deduped,
            hash_columns=[
                "customer_number",
                "customer_name",
                "customer_status",
                "credit_status",
                "is_deleted"
            ]
        )

        df_audited = add_silver_audit_columns(df_hashed)

        print("=" * 80)
        print("After Standardize, Defaults, Dedup, Hash, Audit")
        df_audited.show(truncate=False)

        null_pk_count, duplicate_pk_count = validate_primary_key(
            df=df_audited,
            primary_key="customer_id"
        )

        print("=" * 80)
        print("Primary Key Validation")
        print("Null primary key count:", null_pk_count)
        print("Duplicate primary key count:", duplicate_pk_count)

        valid_df, invalid_df = split_valid_invalid_records(
            df=df_audited,
            required_columns=["customer_id", "customer_number"]
        )

        print("=" * 80)
        print("Valid Records")
        valid_df.show(truncate=False)

        print("=" * 80)
        print("Invalid Records")
        invalid_df.show(truncate=False)

        print("=" * 80)
        print("Counts")
        print("Original count:", df.count())
        print("Deduped count:", df_deduped.count())
        print("Valid count:", valid_df.count())
        print("Invalid count:", invalid_df.count())

    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()