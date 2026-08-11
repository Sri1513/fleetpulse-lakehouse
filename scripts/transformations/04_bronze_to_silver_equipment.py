from scripts.utils.config import (
    BRONZE_BASE_PATH,
    SILVER_BASE_PATH
)
from scripts.utils.spark_session import create_spark_session

import pyspark.sql.functions as F
from pyspark.sql.window import Window

def load_to_silver(spark, bronze_path, silver_path):
    df = spark.read.parquet(bronze_path)

    df_clean = (
            df
            .withColumn("equipment_number",F.upper(F.trim(F.col("equipment_number"))))
            .withColumn("serial_number", F.upper(F.trim(F.col("serial_number"))))
            .withColumn("manufacturer", F.trim(F.col("manufacturer")))
            .withColumn("model", F.trim(F.col("model")))
            .withColumn("equipment_status",F.trim(F.upper(F.coalesce(F.col("equipment_status"),F.lit("UNKNOWN")))))
            .withColumn("telematics_enabled",F.coalesce(F.col("telematics_enabled"),F.lit(False)))
            .withColumn("source_system",F.trim(F.upper(F.col("source_system"))))
            .withColumn("is_deleted",F.coalesce(F.col("is_deleted"),F.lit(False)))
            .withColumn("source_table",F.trim(F.col("source_table")))
            .withColumn("bronze_load_type", F.trim(F.upper(F.col("bronze_load_type"))))
    )

    window_spec = (
                Window
                .partitionBy("equipment_id")
                .orderBy(
                    F.col("updated_at").desc(),
                    F.col("ingestion_timestamp").desc()
                )
            )
    df_dedup = (
            df_clean
            .withColumn("record_num",F.row_number().over(window_spec))
            .filter(F.col("record_num") == 1)
            .drop(F.col("record_num"))
    )

    df_hash = (
            df_dedup
            .withColumn("record_hash", 
                        F.sha2(
                            F.concat_ws(
                                "|",
                                F.col("category_id"),
                                F.col("owning_branch_id"),
                                F.col("current_branch_id"),
                                F.col("model_year"),
                                F.col("acquisition_date"),
                                F.col("acquisition_cost"),
                                F.col("telematics_enabled"),
                                F.col("is_deleted"),
                                F.col("equipment_number"),
                                F.col("serial_number"),
                                F.col("manufacturer"),
                                F.col("model"),
                                F.col("equipment_status")
                            ),256))
    )
    silver_df = (
                df_hash
                .withColumn("silver_created_timestamp", F.current_timestamp())
                .withColumn("silver_updated_timestamp", F.current_timestamp())
                .withColumn("silver_quality_status", F.lit("VALID"))
                )

    silver_df.write.mode("overwrite").parquet(silver_path)
    print(f"Data has been writen in this path: {silver_path}")

def validation(spark,bronze_path,silver_path):
    print(f"Bronze path:{bronze_path}")
    print(f"Silver Path: {silver_path}")
    bronze_df = spark.read.parquet(bronze_path)
    silver_df = spark.read.parquet(silver_path)
    print("Bronze count:", bronze_df.count())
    print("Silver Count", silver_df.count())
    distinct_equipment_count = silver_df.select("equipment_id").distinct().count()
    print(f"Distinct equipment_id count : {distinct_equipment_count}")

def main() :
    spark = None
    try:
        spark = create_spark_session("LoadingTheEquipementToSilver")
        table = "equipment"
        bronze_path = f"{BRONZE_BASE_PATH}/{table}"
        silver_path = f"{SILVER_BASE_PATH}/{table}"
        load_to_silver(spark,bronze_path,silver_path)
        validation(spark,bronze_path,silver_path)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()