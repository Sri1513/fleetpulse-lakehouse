import pyspark.sql.functions as F
from pyspark.sql.window import Window
from functools import reduce
import operator

def deduplicate_latest(df,key_columns,order_columns):

    window_spec = (
                    Window
                    .partitionBy(*[F.col(key) for key in key_columns])
                    .orderBy(*[F.col(column).desc() for column in order_columns])
                )
    dedup_df = (
                df
                .withColumn("row_num",F.row_number().over(window_spec))
                .filter(F.col("row_num") == 1)
                .drop(F.col("row_num"))  
                )
    return dedup_df

def standardize_columns(df,trim_columns,uppercase_columns):
    trim_df = df
    for column in trim_columns:
        trim_df = (
                    trim_df
                    .withColumn(column, F.trim(F.col(column)))
        )
    transformed_df = trim_df
    for column in uppercase_columns:
        transformed_df = (
                    transformed_df
                    .withColumn(column, F.upper(F.trim(F.col(column))))
        )
    return transformed_df

def apply_defaults(df,defaults):
    transformed_df = df
    for key in defaults:
        transformed_df = (
                            transformed_df
                            .withColumn(key,F.coalesce(F.col(key),F.lit(defaults[key])))
        )
    return transformed_df

def add_record_hash(df, hash_columns, hash_column_name = "record_hash"):
    """
    Adds a SHA-256 hash column based on a dynamic list of hash columns.
    """
    hash_df = df.withColumn(
        hash_column_name, 
        F.sha2(
            F.concat_ws("|", *[F.col(c).cast("string") for c in hash_columns]), 
            256  # Specify bit length for sha2 (e.g., 256)
        )
    )
    return hash_df

def add_silver_audit_columns(df, quality_status="VALID" ):
    transformed_df = (
                        df
                        .withColumn("silver_created_timestamp", F.current_timestamp())
                        .withColumn("silver_updated_timestamp", F.current_timestamp())
                        .withColumn("silver_quality_status", F.lit(quality_status))
    )
    return transformed_df

def validate_primary_key(df,primary_key):
    null_filtered_df = (
                        df
                        .filter(F.col(primary_key).isNull())
    )
    non_null_df = (
                    df
                    .filter(F.col(primary_key).isNotNull())
    )
    null_primary_key_count = null_filtered_df.count()
    duplicate_filtered_df = (
                            non_null_df
                            .groupBy(primary_key)
                            .agg(F.count(F.col(primary_key)).alias("primary_key_count"))
                            .filter(F.col("primary_key_count") > 1)
    )
    duplicate_primary_key_count = duplicate_filtered_df.count()
    return {
        "null_primary_key_count" : null_primary_key_count, 
        "dupe_primary_key_count" : duplicate_primary_key_count
        }

def split_valid_invalid_records(df, required_columns):
    """
    Splits a DataFrame into valid and invalid records.

    A record is invalid if any required column is null.
    A record is valid if all required columns are non-null.
    """

    if not required_columns:
        return df, df.limit(0)

    invalid_condition = reduce(
        operator.or_,
        [F.col(column).isNull() for column in required_columns]
    )

    invalid_df = df.filter(invalid_condition)
    valid_df = df.filter(~invalid_condition)

    return {"valid" : valid_df, "invalid" : invalid_df}

def apply_referential_checks(spark,df,referential_checks,silver_base_path):
    result_df = df
    for check in referential_checks:
        parent_table = check['parent_table']
        parent_key =  check['parent_key']
        child_key = check['child_key']
        parent_df = spark.read.parquet(f"{silver_base_path}/{parent_table}")
        parent_key_df = (
                        parent_df
                        .select(F.col(parent_key).alias("__parent_key"))
                        .distinct()
                        )
        result_df = (
                    result_df
                    .join(
                        parent_key_df,
                        parent_key_df['__parent_key'] == result_df[child_key],
                        "inner")
                    .drop(F.col("__parent_key")))
    return result_df