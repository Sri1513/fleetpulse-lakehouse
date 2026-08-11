from dotenv import load_dotenv
import os 

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, current_date, lit

load_dotenv()

postgres_host = os.getenv("POSTGRES_HOST")
postgres_port = os.getenv("POSTGRES_PORT")
postgres_db = os.getenv("POSTGRES_DB")
postgres_schema = os.getenv("POSTGRES_SCHEMA")
postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")

minio_endpoint = os.getenv("MINIO_ENDPOINT")
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")
minio_bucket = os.getenv("MINIO_BUCKET")

jdbc_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"

spark = (SparkSession.builder
         .appName("FleetPulse Branches Bronze Ingestion")
         .master("local[*]")
         .config(
                "spark.jars.packages",
                "org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.5.0"
                )
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
        )

source_table = "branches"

full_table_name = f"{postgres_schema}.{source_table}"

bronz_path = f"s3a://{minio_bucket}/bronze/rental_oltp/{source_table}"

print(f"Reatinf source table:{full_table_name}")

read_df = (spark.read
    .format("jdbc")
    .option("url",jdbc_url)
    .option("dbtable", full_table_name)
    .option("user",postgres_user)
    .option("password",postgres_password)
    .option("driver","org.postgresql.Driver")
    .load()
)

bronze_df = (
    read_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("ingestion_date", current_date())
    .withColumn("source_table", lit(source_table))
    .withColumn("bronze_load_type", lit("FULL_LOAD"))
)

print("Source row count:", bronze_df.count())

print(f"Writing Bronze data to: {bronz_path}")

bronze_df.write.mode("overwrite").parquet(bronz_path)

print("Bronze write completed.")

bronze_df = spark.read.parquet(bronz_path)

print("Bronze row count:", bronze_df.count())
bronze_df.show(truncate=False)

spark.stop()