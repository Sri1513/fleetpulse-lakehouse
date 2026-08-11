from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("FleetPulse Hadoop Version Test")
    .master("local[*]")
    .getOrCreate()
)

hadoop_version = spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion()

print("Spark version:", spark.version)
print("Hadoop version:", hadoop_version)

spark.stop()