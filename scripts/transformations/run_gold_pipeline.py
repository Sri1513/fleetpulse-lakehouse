from scripts.utils.spark_session import ( 
    create_spark_session,
    create_delta_spark_session
)
from scripts.utils.config import (
    GOLD_BASE_PATH,
    SILVER_BASE_PATH
    )
from scripts.utils.gold_utils import (
    add_gold_audit_columns,
    validate_dimension,
    build_incoming_dimension,
    initial_load,
    incremental_load,
    write_initial_delta_table,
    expire_changed_rows,
    append_gold_table
    
)
from scripts.config.gold_table_config import GOLD_TABLE_CONFIG

import pyspark.sql.functions as F
from delta.tables import DeltaTable

def build_dim_date(spark, start_date, end_date):
    date_df = spark.sql(f"""
        SELECT explode(
            sequence(
                to_date('{start_date}'),
                to_date('{end_date}'),
                interval 1 day
            )
        ) AS full_date
    """)

    dim_date_df = (
        date_df
        .withColumn("date_key", F.date_format(F.col("full_date"), "yyyyMMdd").cast("int"))
        .withColumn("year", F.year(F.col("full_date")))
        .withColumn("quarter", F.quarter(F.col("full_date")))
        .withColumn("month", F.month(F.col("full_date")))
        .withColumn("month_name", F.date_format(F.col("full_date"), "MMMM"))
        .withColumn("month_short_name", F.date_format(F.col("full_date"), "MMM"))
        .withColumn("week_of_year", F.weekofyear(F.col("full_date")))
        .withColumn("day_of_month", F.dayofmonth(F.col("full_date")))
        .withColumn("day_of_week", F.dayofweek(F.col("full_date")))
        .withColumn("day_name", F.date_format(F.col("full_date"), "EEEE"))
        .withColumn(
            "is_weekend",
            F.when(F.dayofweek(F.col("full_date")).isin(1, 7), F.lit(True)).otherwise(F.lit(False))
        )
    )

    dim_date_df = dim_date_df.select(
        "date_key",
        "full_date",
        "year",
        "quarter",
        "month",
        "month_name",
        "month_short_name",
        "week_of_year",
        "day_of_month",
        "day_of_week",
        "day_name",
        "is_weekend"
    )

    dim_date_df = add_gold_audit_columns(dim_date_df)

    return dim_date_df


def process_dim_date(spark):
    gold_table_name = "dim_date"
    gold_path = f"{GOLD_BASE_PATH}/{gold_table_name}"

    dim_date_df = build_dim_date(
        spark=spark,
        start_date="2020-01-01",
        end_date="2035-12-31"
    )

    write_initial_delta_table(dim_date_df, gold_path)


def process_dimension(incoming_df, spark, gold_path,dimension_config,force_rebuild=False):
    table_name = dimension_config['gold_table_name']
    table_path = f"{gold_path}/{table_name}"

    table_exits = DeltaTable.isDeltaTable(spark,table_path)

    if table_exits and not force_rebuild:
        delta_df = spark.read.format("delta").load(table_path)

        return incremental_load(
            incoming_df=incoming_df,
            gold_df=delta_df,
            table_config=dimension_config
            )
    
    initial_df = initial_load(
            incoming_df=incoming_df, 
            dimension_config=dimension_config
            )
    if table_exits and force_rebuild:
        (
            initial_df
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(table_path)
        )
    else:
        write_initial_delta_table(initial_df, table_path)
    delta_df = spark.read.format("delta").load(table_path)
        
    return {
            "load_type": "initial",
            "counts": {
                        "incoming_count": incoming_df.count(),
                        "inserted_count": delta_df.count()
                        },
                "df": delta_df
                }

def print_dimension_summary(dimension_name, results):
    load_type = results["load_type"]
    counts = results["counts"]

    if load_type == "initial":
        print(
            f"[PASS] {dimension_name}: "
            f"initial, "
            f"incoming={counts['incoming_count']}, "
            f"inserted={counts['inserted_count']}"
        )

    elif load_type == "incremental":
        print(
            f"[PASS] {dimension_name}: "
            f"incremental, "
            f"incoming={counts['incoming_count']}, "
            f"changed={counts['changed_count']}, "
            f"new={counts['new_count']}, "
            f"inserted={counts['rows_to_insert_count']}"
        )

def main():
    spark = None
    table_config = GOLD_TABLE_CONFIG
    try:
        spark = create_delta_spark_session("FleetPulse Gold Pipeline")
        dim_date_path = f"{GOLD_BASE_PATH}/dim_date"
        table_exits = DeltaTable.isDeltaTable(spark,dim_date_path)
        if table_exits:
            saved_df = spark.read.format("delta").load(dim_date_path)

            validation_results = validate_dimension(
                    df=saved_df,
                    surrogate_key_column="date_key",
                    natural_key_column="full_date"
                )

            print("=" * 80)
            print("GOLD TABLE COMPLETED: dim_date")
            print("=" * 80)
            print(f"Gold path: {dim_date_path}")
            print(validation_results)
        else:
            process_dim_date(spark)
        dimensions_to_process = [
                "dim_branch",
                "dim_customer",
                "dim_equipment_category",
                "dim_equipment",
                ]
        for dimension_name in dimensions_to_process:
            dimension_config = table_config[dimension_name]
            gold_table_name = dimension_config["gold_table_name"]
            gold_table_path = f"{GOLD_BASE_PATH}/{gold_table_name}"
            incoming_df = build_incoming_dimension(
                spark=spark,
                dimension_config=table_config[dimension_name],
                silver_base_path=SILVER_BASE_PATH
                )
            results = process_dimension(
                incoming_df=incoming_df,
                spark=spark,
                gold_path=GOLD_BASE_PATH,
                dimension_config=table_config[dimension_name],
                force_rebuild=False)
            print_dimension_summary(dimension_name, results)
            if results["load_type"] == "incremental":
                if results["counts"]["changed_count"] > 0:
                    expire_changed_rows(
                        spark=spark,
                        changed_df=results["changed_df"],
                        gold_path=GOLD_BASE_PATH,
                        table_config=dimension_config
                        )
                if results["counts"]["rows_to_insert_count"] > 0:
                    append_gold_table(
                        df=results["rows_to_insert_with_keys_df"],
                        gold_path=gold_table_path
                        )
                else:
                    print(f"[INFO] {dimension_name}: no SCD2 changes detected.")
            gold_dim_table = spark.read.format("delta").load(gold_table_path)
            dim_validation_results = validate_dimension(
                    df=gold_dim_table,
                    surrogate_key_column=table_config[dimension_name]["surrogate_key"],
                    natural_key_column=table_config[dimension_name]["natural_key"]
                    )
            print(
                f"[VALIDATION] {dimension_name}: "
                f"rows={dim_validation_results['total_count']}, "
                f"null_sk={dim_validation_results['null_surrogate_key_count']}, "
                f"dup_sk={dim_validation_results['duplicate_surrogate_key_count']}, "
                f"null_nk={dim_validation_results['null_natural_key_count']}, "
                f"dup_nk={dim_validation_results['duplicate_natural_key_count']}"
                )  
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()