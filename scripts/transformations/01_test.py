from copy import deepcopy
import pyspark.sql.functions as F

from scripts.utils.config import SILVER_BASE_PATH, GOLD_BASE_PATH
from scripts.config.gold_table_config import GOLD_TABLE_CONFIG
from scripts.utils.gold_utils import (
    build_incoming_dimension,
    initial_load,
    incremental_load,
    expire_changed_rows,
    append_gold_table
)

from scripts.utils.spark_session import create_delta_spark_session

spark = create_delta_spark_session(
    app_name="TestingSCD"
)
# -------------------------------------------------------------------
# 1. Create test config so we do not touch real dim_branch
# -------------------------------------------------------------------

test_config = deepcopy(GOLD_TABLE_CONFIG["dim_branch"])
test_config["gold_table_name"] = "dim_branch_scd2_test"

test_gold_path = f"{GOLD_BASE_PATH}/{test_config['gold_table_name']}"

print("Test Gold path:", test_gold_path)


# -------------------------------------------------------------------
# 2. Build clean incoming dimension from real Silver branches
# -------------------------------------------------------------------

incoming_df = build_incoming_dimension(
    spark=spark,
    silver_base_path=SILVER_BASE_PATH,
    dimension_config=GOLD_TABLE_CONFIG["dim_branch"]
)

print("Original incoming Silver dimension")
incoming_df.show(truncate=False)


# -------------------------------------------------------------------
# 3. Create/rebuild initial test Gold dimension
#    This overwrite is only for the test table.
# -------------------------------------------------------------------

initial_test_dim_df = initial_load(
    incoming_df=incoming_df,
    dimension_config=test_config
)

(
    initial_test_dim_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(test_gold_path)
)

print("Initial test dim_branch_scd2_test created")

gold_df = spark.read.format("delta").load(test_gold_path)

print("Initial Gold test dimension")
gold_df.orderBy("branch_key").show(truncate=False)


# -------------------------------------------------------------------
# 4. Create test incoming data:
#    - branch_id = 1 changed
#    - branch_id = 256 new
# -------------------------------------------------------------------

changed_existing_df = (
    incoming_df
    .withColumn(
        "branch_name",
        F.when(
            F.col("branch_id") == 1,
            F.lit("Youngstown Rental Center Updated")
        ).otherwise(F.col("branch_name"))
    )
    .withColumn(
        "record_hash",
        F.when(
            F.col("branch_id") == 1,
            F.lit("TEST_HASH_BRANCH_1_CHANGED")
        ).otherwise(F.col("record_hash"))
    )
)

new_branch_df = (
    incoming_df
    .limit(1)
    .select(
        F.lit(256).cast("bigint").alias("branch_id"),
        F.lit("BR-OH-999").alias("branch_code"),
        F.lit("Akron Rental Test Branch").alias("branch_name"),
        F.lit("Midwest").alias("region"),
        F.lit("Ohio Valley").alias("district"),
        F.lit("Akron").alias("city"),
        F.lit("OH").alias("state"),
        F.lit("USA").alias("country"),
        F.lit("44308").alias("postal_code"),
        F.lit(41.0814).cast("decimal(10,6)").alias("latitude"),
        F.lit(-81.5190).cast("decimal(10,6)").alias("longitude"),
        F.lit("ACTIVE").alias("branch_status"),
        F.lit("RENTAL_OLTP").alias("source_system"),
        F.lit("TEST_HASH_BRANCH_256_NEW").alias("record_hash")
    )
)

test_incoming_df = changed_existing_df.unionByName(new_branch_df)

print("Test incoming data")
test_incoming_df.orderBy("branch_id").show(truncate=False)


# -------------------------------------------------------------------
# 5. Run SCD2 classification and key assignment
# -------------------------------------------------------------------

results = incremental_load(
    incoming_df=test_incoming_df,
    gold_df=gold_df,
    table_config=test_config
)

print("Classification counts")
print(results["counts"])

print("Changed rows")
results["changed_df"].show(truncate=False)

print("New rows")
results["new_df"].show(truncate=False)

print("Rows to insert with new surrogate keys")
results["rows_to_insert_with_keys_df"].orderBy("branch_key").show(truncate=False)


# -------------------------------------------------------------------
# 6. Apply SCD2 actions:
#    - expire changed current rows
#    - append changed/new rows
# -------------------------------------------------------------------

if results["counts"]["changed_count"] > 0:
    expire_changed_rows(
        spark=spark,
        changed_df=results["changed_df"],
        gold_path=GOLD_BASE_PATH,
        table_config=test_config
    )
    print("Expired changed rows")

if results["counts"]["rows_to_insert_count"] > 0:
    append_gold_table(
        df=results["rows_to_insert_with_keys_df"],
        gold_path=test_gold_path
    )
    print("Appended new SCD2 rows")


# -------------------------------------------------------------------
# 7. Read final test dimension and validate expected behavior
# -------------------------------------------------------------------

final_test_dim_df = spark.read.format("delta").load(test_gold_path)

print("Final test dimension")
final_test_dim_df.orderBy("branch_id", "branch_key").show(truncate=False)

print("Validation checks")

total_count = final_test_dim_df.count()

current_count = (
    final_test_dim_df
    .filter(F.col("is_current") == True)
    .count()
)

branch_1_total = (
    final_test_dim_df
    .filter(F.col("branch_id") == 1)
    .count()
)

branch_1_current = (
    final_test_dim_df
    .filter((F.col("branch_id") == 1) & (F.col("is_current") == True))
    .count()
)

branch_256_total = (
    final_test_dim_df
    .filter(F.col("branch_id") == 256)
    .count()
)

duplicate_current_natural_keys = (
    final_test_dim_df
    .filter(F.col("is_current") == True)
    .groupBy("branch_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("total_count:", total_count)
print("current_count:", current_count)
print("branch_1_total:", branch_1_total)
print("branch_1_current:", branch_1_current)
print("branch_256_total:", branch_256_total)
print("duplicate_current_natural_keys:", duplicate_current_natural_keys)