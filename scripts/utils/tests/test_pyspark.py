from pyspark.sql import SparkSession
from scripts.utils.config import(
    GOLD_BASE_PATH
)

spark = (SparkSession.builder
         .appName("TestConnection")
         .master("local[*]")
         .config("spark.jars.packages", "io.delta:delta-spark_4.1_2.13:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaSparkCatalog")
         .getOrCreate()
         )

data = [
(1, "branches"),
(2, "customers"),
(3, "equipment")
]

columns = ["table_id", "table_name"]
df = spark.createDataFrame(data, columns)
print(GOLD_BASE_PATH)
print("PySpark session created successfully.")
df.show(truncate=False)
print("Spark version:", spark.version)
spark.stop()