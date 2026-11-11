import os
from pyspark.sql import SparkSession
from pathlib import Path
from scripts.utils.config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY
)

project_root = Path(__file__).resolve().parents[2]
log4j_path = project_root / "config" / "log4j2.properties"
log4j_uri = log4j_path.as_uri()

JARS = [
    os.path.join(project_root, "jars", "org.postgresql_postgresql-42.7.3.jar"),
    os.path.join(project_root, "jars", "org.checkerframework_checker-qual-3.42.0.jar"),
    os.path.join(project_root, "jars", "org.apache.hadoop_hadoop-aws-3.5.0.jar"),
    os.path.join(project_root, "jars", "software.amazon.awssdk_bundle-2.35.4.jar"),
    os.path.join(project_root, "jars", "software.amazon.s3.analyticsaccelerator_analyticsaccelerator-s3-1.3.1.jar"),
    os.path.join(project_root, "jars", "org.wildfly.openssl_wildfly-openssl-2.2.5.Final.jar"),
]

def create_spark_session(app_name):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars", ",".join(JARS))
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.extraJavaOptions", f"-Dlog4j.configurationFile={log4j_uri}")
        .config("spark.executor.extraJavaOptions", f"-Dlog4j.configurationFile={log4j_uri}")
        .getOrCreate()
    )


    spark.sparkContext.setLogLevel("ERROR")

    return spark
def create_delta_spark_session(app_name):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[1]")
        .config("spark.jars", ",".join(JARS))

        # Delta Lake package
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_4.1_2.13:4.3.1"
        )

        # Delta SQL support
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )

        # Existing MinIO/S3A configs
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        # Logging configs
        .config("spark.driver.extraJavaOptions", f"-Dlog4j.configurationFile={log4j_uri}")
        .config("spark.executor.extraJavaOptions", f"-Dlog4j.configurationFile={log4j_uri}")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark