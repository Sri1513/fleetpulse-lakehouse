from scripts.utils.config import (
    BRONZE_BASE_PATH,
    SILVER_BASE_PATH
)
from scripts.utils.spark_session import create_spark_session

import pyspark.sql.functions as F
from pyspark.sql.window import Window

def load_to_silver(spark, bronze_path, silver_path):
    df = spark.read.parquet(bronze_path)
    df_clean = (df
            .withColumn("category_code", F.upper(F.trim(F.col("category_code"))))
            .withColumn("category_name", F.trim(F.col("category_name")))
            .withColumn("equipment_class", F.trim(F.col("equipment_class")))
            .withColumn("equipment_type", F.trim(F.col("equipment_type")))
            .withColumn("source_table", F.trim(F.col("source_table")))
            .withColumn("source_system", F.upper(F.trim(F.col("source_system"))))
            .withColumn("bronze_load_type", F.upper(F.trim(F.col("bronze_load_type"))))
            .withColumn("is_deleted", F.coalesce(F.col("is_deleted"),F.lit(False)))
        )
    window_spec = (
                Window
                .partitionBy("category_id")
                .orderBy(
                    F.col("updated_at").desc(),
                    F.col("ingestion_timestamp").desc())
            )

    df_dedup = (
            df_clean
            .withColumn("row_num", F.row_number().over(window_spec))
            .filter(F.col("row_num") == 1)
            .drop(F.col("row_num"))
            )
    df_hash = (
        df_dedup
        .withColumn("record_hash", 
                    F.sha2(
                        F.concat_ws(
                                    "|",
                                    F.col("category_code"),
                                    F.col("category_name"),
                                    F.col("parent_category_id").cast("string"),
                                    F.col("equipment_class"),
                                    F.col("equipment_type"),
                                    F.col("is_deleted").cast("string")
                        ),256
                    ))    
            )
    silver_df = (
                df_hash
                .withColumn("silver_created_timestamp", F.current_timestamp())
                .withColumn("silver_updated_timestamp", F.current_timestamp())
                .withColumn("silver_quality_status", F.lit("VALID"))
            )
    silver_df.write.mode("overwrite").parquet(silver_path)
    print(f"Data ha writen in this path: {silver_path}")

def validation(spark,bronze_path,silver_path):
    print(f"Bronze path:{bronze_path}")
    print(f"Silver Path: {silver_path}")
    bronze_df = spark.read.parquet(bronze_path)
    silver_df = spark.read.parquet(silver_path)
    print("Bronze count:", bronze_df.count())
    print("Silver Count", silver_df.count())
    distint_customer_count = silver_df.select("category_id").distinct().count()
    print(f"Distinct category_id count : {distint_customer_count}")



def main() :
    spark = None
    try:
        spark = create_spark_session("LoadingTheequipment_categoryToSilver")
        table = "equipment_categories"
        bronze_path = f"{BRONZE_BASE_PATH}/{table}"
        silver_path = f"{SILVER_BASE_PATH}/{table}"
        load_to_silver(spark,bronze_path,silver_path)
        validation(spark,bronze_path,silver_path)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()