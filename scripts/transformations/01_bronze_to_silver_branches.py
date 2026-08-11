from scripts.utils.spark_session import create_spark_session
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    coalesce,
    lit,
    sha2,
    concat_ws,
    current_timestamp,
    row_number,
)

from pyspark.sql.window import Window

from scripts.utils.config import(
    BRONZE_BASE_PATH,
    SILVER_BASE_PATH
)

SOURCE_TABLES = [
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
    "equipment_status_history",
]

spark = create_spark_session("Loading the table to Silver")

def load_to_silver(bronze_path,silver_path):
    df = spark.read.parquet(bronze_path)
    df_clean = (
        df
        .withColumn("branch_code", upper(trim(col("branch_code"))))
        .withColumn("branch_name", trim(col("branch_name")))
        .withColumn("region", trim(col("region")))
        .withColumn("district", trim(col("district")))
        .withColumn("city", trim(col("city")))
        .withColumn("state", upper(trim(col("state"))))
        .withColumn("country", upper(trim(coalesce(col("country"), lit("USA")))))
        .withColumn("postal_code", trim(col("postal_code")))
        .withColumn("branch_status", upper(trim(coalesce(col("branch_status"), lit("UNKNOWN")))))
        .withColumn("source_system", upper(trim(col("source_system"))))
        .withColumn("is_deleted", coalesce(col("is_deleted"), lit(False)))
    )
    df_hashed = df_clean.withColumn(
            "record_hash",
            sha2(
                concat_ws(
                    "|",
                    col("branch_code"),
                    col("branch_name"),
                    col("region"),
                    col("district"),
                    col("city"),
                    col("state"),
                    col("country"),
                    col("postal_code"),
                    col("latitude").cast("string"),
                    col("longitude").cast("string"),
                    col("branch_status"),
                    col("is_deleted").cast("string")
                ),
                256
            )
        )
    df_silver = (
        df_hashed
        .withColumn("silver_created_timestamp", current_timestamp())
        .withColumn("silver_updated_timestamp", current_timestamp())
        .withColumn("silver_quality_status", lit("VALID"))
    )

    window_spec = (
        Window
        .partitionBy("branch_id")
        .orderBy(
            col("updated_at").desc(),
            col("ingestion_timestamp").desc()
        )
    )

    df_deduped = (
        df_silver
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    df_deduped.write.mode("overwrite").parquet(silver_path)

    print(f"Silver Path : {silver_path}")
    silver_df = spark.read.parquet(silver_path)
    print("Bronz Count:", df.count())
    print("Silver count:", silver_df.count())
    silver_df.select(
    "branch_id",
    "branch_code",
    "branch_name",
    "state",
    "country",
    "branch_status",
    "record_hash",
    "silver_quality_status"
    ).show(truncate=False)
    bronze_count = df.count()
    silver_count = df_deduped.count()
    distinct_branch_count = df_deduped.select("branch_id").distinct().count()

    print("Bronze count:", bronze_count)
    print("Silver count:", silver_count)
    print("Distinct branch_id count:", distinct_branch_count)

table = "branches"
bronze_path = f"{BRONZE_BASE_PATH}/{table}"
silver_path = f"{SILVER_BASE_PATH}/{table}"
load_to_silver(bronze_path,silver_path)
silver_df = spark.read.parquet(silver_path)
silver_df.show(truncate=False)