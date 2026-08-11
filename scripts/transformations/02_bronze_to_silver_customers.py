from scripts.utils.config import (
    BRONZE_BASE_PATH,
    SILVER_BASE_PATH
)
from scripts.utils.spark_session import create_spark_session

import pyspark.sql.functions as F
from pyspark.sql.window import Window


def load_to_silver(spark, bronze_path, silver_path):

    df = spark.read.parquet(bronze_path)
    clean_df = (
        df
        .withColumn("customer_number", F.upper(F.trim(F.col("customer_number"))))
        .withColumn("customer_name", F.trim(F.col("customer_name")))
        .withColumn("customer_type", F.upper(F.trim(F.col("customer_type"))))
        .withColumn("industry", F.trim(F.col("industry")))
        .withColumn("contact_email", F.trim(F.col("contact_email")))
        .withColumn("contact_phone", F.trim(F.col("contact_phone")))
        .withColumn("billing_city", F.trim(F.col("billing_city")))
        .withColumn("billing_state", F.upper(F.trim(F.col("billing_state"))))
        .withColumn("billing_country", F.upper(F.trim(F.coalesce(F.col("billing_country"), F.lit("USA")))))
        .withColumn("credit_status", F.upper(F.trim(F.coalesce(F.col("credit_status"), F.lit("UNKNOWN")))))
        .withColumn("customer_status", F.upper(F.trim(F.coalesce(F.col("customer_status"),F.lit("UNKNOWN")))))
        .withColumn("source_system", F.upper(F.trim(F.col("source_system"))))
        .withColumn("is_deleted", F.coalesce(F.col("is_deleted"),F.lit(False)))
        )

    window_spec = (
        Window
        .partitionBy(F.col("customer_id"))
        .orderBy(
            F.col("updated_at").desc(),
            F.col("ingestion_timestamp").desc()
            )
    )

    df_dedup = (
        clean_df
        .withColumn("row_number", F.row_number().over(window_spec))
        .filter(F.col("row_number") == 1)
        .drop(F.col("row_number"))
    )
    df_hash = df_dedup.withColumn(
        "record_hash", F.sha2(
            F.concat_ws(    "|",
                            F.col("customer_number"),
                            F.col("customer_name"),
                            F.col("customer_type"),
                            F.col("industry"),
                            F.col("billing_city"),
                            F.col("billing_state"),
                            F.col("billing_country"),
                            F.col("credit_status"),
                            F.col("customer_status"),
                            F.col("is_deleted").cast("string"),)
                        ,256),
                    )
    silver_df = (
                df_hash
                .withColumn("silver_created_timestamp", F.current_timestamp())
                .withColumn("silver_updated_timestamp", F.current_timestamp())
                .withColumn("silver_quality_status", F.lit("VALID"))
                )
    silver_df.write.mode("overwrite").parquet(silver_path)
    print(f"Bronze path:{bronze_path}")
    print(f"Silver Path: {silver_path}")
    print("Bronze count:", df.count())
    print("Silver Count", silver_df.count())
    distint_customer_count = silver_df.select("customer_id").distinct().count()
    print(f"Distint customer count : {distint_customer_count}")
    silver_df.printSchema()

def main() :
    spark = None
    try:
        spark = create_spark_session("LoadingTheCustomersToSilver")
        table = "customers"
        bronze_path = f"{BRONZE_BASE_PATH}/{table}"
        silver_path = f"{SILVER_BASE_PATH}/{table}"
        load_to_silver(spark,bronze_path,silver_path)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()
