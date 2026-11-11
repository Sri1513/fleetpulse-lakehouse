import pyspark.sql.functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable


def add_gold_audit_columns(df):
    return (
        df
        .withColumn("gold_created_timestamp", F.current_timestamp())
        .withColumn("gold_updated_timestamp", F.current_timestamp())
    )
def write_initial_delta_table(df, gold_path):
    (
        df
        .write
        .mode("errorifexists")
        .format("delta")
        .save(gold_path)
    )

def append_gold_table(df, gold_path):
    (
        df
        .write
        .mode("append")
        .format("delta")
        .save(gold_path)
    )

def validate_dimension(df, surrogate_key_column, natural_key_column):
    total_count = df.count()

    null_surrogate_key_count = (
        df
        .filter(F.col(surrogate_key_column).isNull())
        .count()
    )
    duplicate_surrogate_key_count = (
        df
        .groupBy(surrogate_key_column)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    null_natural_key_count = (
        df
        .filter(F.col(natural_key_column).isNull())
        .count()
    )
    duplicate_natural_key_count = (
        df
        .groupBy(natural_key_column)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    return {
        "total_count": total_count,
        "null_surrogate_key_count": null_surrogate_key_count,
        "duplicate_surrogate_key_count": duplicate_surrogate_key_count,
        "null_natural_key_count": null_natural_key_count,
        "duplicate_natural_key_count": duplicate_natural_key_count
    }
def build_incoming_dimension(spark, silver_base_path ,dimension_config ):
    silver_path = f"{silver_base_path}/{dimension_config['source_table']}"
    incoming_df = spark.read.parquet(silver_path)
    return (incoming_df.select(*dimension_config['business_columns']))

def type_2(df):
    scd_df = (
        df
        .withColumn("effective_start_date", F.current_date())
        .withColumn("effective_end_date", F.to_date(F.lit("9999-12-31")))
        .withColumn("is_current", F.lit(True))
    )
    return scd_df

def type_1():
    pass

scd_registry = {
    "type_1": type_1,
    "type_2": type_2
        }

def initial_load(incoming_df,dimension_config):

    window_spec = Window.orderBy(dimension_config['natural_key'])
    gold_sug_df = (
            incoming_df
            .withColumn(dimension_config['surrogate_key'], F.row_number().over(window_spec))
    )
    function_call = dimension_config['scd_type']
    gold_scd_df = scd_registry[function_call](gold_sug_df)
    gold_audit_df = add_gold_audit_columns(gold_scd_df)
    gold_df = (gold_audit_df.select(*dimension_config['target_columns']))
    return gold_df

def incremental_load(incoming_df,gold_df,table_config):
    natural_key = table_config["natural_key"]
    surrogate_key = table_config["surrogate_key"]
    hash_column = table_config["hash_column"]
    max_existing_key = gold_df.agg(F.max(F.col(surrogate_key))).collect()[0][0]
    current_dm_df = (
        gold_df
        .filter(F.col("is_current")==True)
        .select(
            natural_key,
            surrogate_key,
            hash_column
        )
        )
    changed_df = (
        incoming_df.alias("silver_table")
        .join(
            current_dm_df.alias("filtered_dm_table"),
            (F.col(f"silver_table.{natural_key}") == F.col(f"filtered_dm_table.{natural_key}"))
            & 
            (F.col(f"silver_table.{hash_column}") != F.col(f"filtered_dm_table.{hash_column}")),
            "inner"
        )
        .select("silver_table.*")
        )
    unchanged_df = (
        incoming_df.alias("silver_table")
        .join(
            current_dm_df.alias("filtered_dm_table"),
            (F.col(f"silver_table.{natural_key}") == F.col(f"filtered_dm_table.{natural_key}"))
            & 
            (F.col(f"silver_table.{hash_column}") == F.col(f"filtered_dm_table.{hash_column}")),
            "inner"
        )
        .select("silver_table.*")
        )
    new_df = (
        incoming_df.alias("silver_table")
        .join(
            current_dm_df.alias("filtered_dm_table"),
            (F.col(f"silver_table.{natural_key}") == F.col(f"filtered_dm_table.{natural_key}")),
            "left_anti"
        )
        )
    rows_to_insert_df = (
        changed_df.unionByName(new_df)
    )
    window_spec = Window.orderBy(F.col(natural_key))
    sk_rows_to_insert_df = (
        rows_to_insert_df
        .withColumn(surrogate_key, F.lit(max_existing_key) + F.row_number().over(window_spec))
    )
    function_call = table_config["scd_type"]
    scd_rows_to_insert_df = scd_registry[function_call](sk_rows_to_insert_df)
    audit_rows_to_insert_df = add_gold_audit_columns(df=scd_rows_to_insert_df)
    final_rows_to_insert_df =(
        audit_rows_to_insert_df
        .select(*table_config["target_columns"])
        )
    result = {
        "incoming_count" : incoming_df.count() ,
        "current_dim_count" : current_dm_df.count(),
        "unchanged_count" : unchanged_df.count(),
        "changed_count" : changed_df.count(),
        "new_count" : new_df.count(),
        "rows_to_insert_count" : final_rows_to_insert_df.count()
    }
    return {
    "load_type": "incremental",
    "counts": result,
    "changed_df": changed_df,
    "new_df": new_df,
    "rows_to_insert_df": rows_to_insert_df,
    "rows_to_insert_with_keys_df": final_rows_to_insert_df
    }

def expire_changed_rows(spark, changed_df, gold_path, table_config):
    natural_key = table_config["natural_key"]
    table_path = f"{gold_path}/{table_config['gold_table_name']}"

    target_delta_table = DeltaTable.forPath(spark,table_path)

    changed_keys_df = (
        changed_df
        .select(natural_key)
        .distinct()
    )
    (
        target_delta_table.alias("target")
        .merge(
            changed_keys_df.alias("source"),
            f"target.{natural_key} = source.{natural_key} AND target.is_current = true"
               )
        .whenMatchedUpdate(
            set={
                 "is_current": F.lit(False),
                 "effective_end_date": F.current_date(),
                "gold_updated_timestamp": F.current_timestamp()
                }
            )
        .execute()
    )