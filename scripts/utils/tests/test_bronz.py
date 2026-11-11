from scripts.utils.spark_session import create_spark_session
from scripts.utils.config import BRONZE_BASE_PATH

spark = create_spark_session("Test for bronz layer")

source_table = "branches"

df = spark.read.parquet(f"{BRONZE_BASE_PATH}\{source_table}")
df.printSchema()
df.show()

spark.stop()