from dotenv import load_dotenv
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

load_dotenv()

minio_endpoint = os.getenv("MINIO_ENDPOINT")
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")
minio_bucket = os.getenv("MINIO_BUCKET")

spark = (SparkSession.builder
         .appName("FleetPulse Spark MinIO Write Test")
         .master("local[*]")
         .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.5.0" )
         .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
         .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
         .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .getOrCreate()
         )

data = [
(1, "spark_to_minio_test", "success"),
(2, "fleetpulse_lake", "bronze_ready")
]

columns = ["test_id", "test_name", "status"]

df = spark.createDataFrame(data, columns)
df = df.withColumn("created_at", current_timestamp())

output_path  = f"s3a://{minio_bucket}/bronze/rental_oltp/_spark_minio_test/"

print("Writing test DataFrame to:")
print(output_path)

df.write.mode("overwrite").parquet(output_path)

print("Write completed successfully.")

read_back_df = spark.read.parquet(output_path)

print("Reading back from MinIO:")
read_back_df.show(truncate=False)

print("Row count:", read_back_df.count())
spark.stop()