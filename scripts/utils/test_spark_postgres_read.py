from dotenv import load_dotenv
from pyspark.sql import SparkSession
import os

load_dotenv()

host = os.getenv("POSTGRES_HOST")
port =  os.getenv("POSTGRES_PORT")
database = os.getenv("POSTGRES_DB")
schema = os.getenv("POSTGRES_SCHEMA")
user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")

jsbc_url = f"jdbc:postgresql://{host}:{port}/{database}"

spark = (SparkSession.builder
         .appName("FleetPulse Spark PostgreSQL Read Test")
         .master("local[*]")
         .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
         .getOrCreate()
         )

table_name = "branches"
full_table_name = f"{schema}.{table_name}"

df = (
    spark.read
    .format("jdbc")
    .option("url", jsbc_url)
    .option("dbtable", full_table_name)
    .option("user", user)
    .option("password", password)
    .option("driver","org.postgresql.Driver")
    .load()
)

df.printSchema()
df.show(10, truncate=False)

print("Row count", df.count())

spark.stop()