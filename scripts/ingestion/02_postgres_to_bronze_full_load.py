from scripts.utils.spark_session import create_spark_session
from scripts.utils.config import (
    POSTGRES_SCHEMA,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    JDBC_URL,
    BRONZE_BASE_PATH
)
from pyspark.sql.functions import current_timestamp, current_date, lit

jdbc_url = JDBC_URL



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

spark = create_spark_session("FleetPulse PostgreSQL to Bronze Full Load")

def ingest_table(table_name):
    print("=" * 80)
    print(f"Starting Bronze full load for table: {table_name}")

    full_table_name = f"{POSTGRES_SCHEMA}.{table_name}"
    bronze_path = f"{BRONZE_BASE_PATH}/{table_name}/"

    print(f"Source table: {full_table_name}")
    print(f"Target path: {bronze_path}")

    # We will add the read/write logic in the next step.

    df = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", full_table_name)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    df_bronze = (
        df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("ingestion_date", current_date())
        .withColumn("source_table", lit(table_name))
        .withColumn("bronze_load_type", lit("FULL_LOAD"))
    )

    source_count = df_bronze.count()
    print(f"Source row count for {table_name}: {source_count}")

    df_bronze.write.mode("overwrite").parquet(bronze_path)

    bronze_count = spark.read.parquet(bronze_path).count()
    print(f"Bronze row count for {table_name}: {bronze_count}")

    if source_count != bronze_count:
        raise Exception(
            f"Row count mismatch for {table_name}: source={source_count}, bronze={bronze_count}"
        )

    print(f"Completed Bronze full load for table: {table_name}")

try:
    for table_name in SOURCE_TABLES:
        ingest_table(table_name)
    print("="*80)
    print("All Bronze full loads completed successfully.")
finally:
    spark.stop()