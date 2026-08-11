from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("TestConnection")
         .master("local[*]")
         .getOrCreate()
         )

data = [
(1, "branches"),
(2, "customers"),
(3, "equipment")
]

columns = ["table_id", "table_name"]
df = spark.createDataFrame(data, columns)
print("PySpark session created successfully.")
df.show()
print("Spark version:", spark.version)
spark.stop()