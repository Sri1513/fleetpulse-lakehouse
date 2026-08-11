# FleetPulse Data Engineering Project

## 1. Project Overview

FleetPulse is a local end-to-end data engineering project that simulates an enterprise equipment rental analytics platform.

The project uses:

- **PostgreSQL** as the OLTP source system
- **PySpark** as the processing engine
- **MinIO** as S3-compatible object storage
- **Parquet** as the lake storage format
- **Bronze, Silver, and Gold** lakehouse-style layers

Current completed stages:

```text
PostgreSQL OLTP Source
        ↓
Bronze Full Load Ingestion
        ↓
MinIO Bronze Parquet
        ↓
Metadata-Driven Silver Pipeline
        ↓
MinIO Silver Parquet
```

The Silver layer currently processes all 11 OLTP source tables through a reusable metadata-driven framework.

---

## 2. Source System

### PostgreSQL Database

```text
Database: fleetpulse_db
Schema: rental_oltp
```

### OLTP Source Tables

```text
branches
customers
equipment_categories
equipment
rental_contracts
rental_contract_lines
invoices
invoice_lines
payments
maintenance_work_orders
equipment_status_history
```

The PostgreSQL database represents the normalized operational source system. It is not used as the warehouse or analytics layer.

---

## 3. Data Lake Storage

### MinIO Bucket

```text
fleetpulse-lake
```

### Data Lake Layout

```text
fleetpulse-lake/
├── bronze/
│   └── rental_oltp/
├── silver/
│   └── rental/
├── gold/
│   └── fleet_analytics/
└── checkpoints/
```

### Current Bronze Paths

```text
s3a://fleetpulse-lake/bronze/rental_oltp/branches
s3a://fleetpulse-lake/bronze/rental_oltp/customers
s3a://fleetpulse-lake/bronze/rental_oltp/equipment_categories
s3a://fleetpulse-lake/bronze/rental_oltp/equipment
s3a://fleetpulse-lake/bronze/rental_oltp/rental_contracts
s3a://fleetpulse-lake/bronze/rental_oltp/rental_contract_lines
s3a://fleetpulse-lake/bronze/rental_oltp/invoices
s3a://fleetpulse-lake/bronze/rental_oltp/invoice_lines
s3a://fleetpulse-lake/bronze/rental_oltp/payments
s3a://fleetpulse-lake/bronze/rental_oltp/maintenance_work_orders
s3a://fleetpulse-lake/bronze/rental_oltp/equipment_status_history
```

### Current Silver Paths

```text
s3a://fleetpulse-lake/silver/rental/branches
s3a://fleetpulse-lake/silver/rental/customers
s3a://fleetpulse-lake/silver/rental/equipment_categories
s3a://fleetpulse-lake/silver/rental/equipment
s3a://fleetpulse-lake/silver/rental/rental_contracts
s3a://fleetpulse-lake/silver/rental/rental_contract_lines
s3a://fleetpulse-lake/silver/rental/invoices
s3a://fleetpulse-lake/silver/rental/invoice_lines
s3a://fleetpulse-lake/silver/rental/payments
s3a://fleetpulse-lake/silver/rental/maintenance_work_orders
s3a://fleetpulse-lake/silver/rental/equipment_status_history
```

---

## 4. Project Structure

```text
fleetpulse_project/
├── .env
├── .venv/
├── config/
│   └── log4j2.properties
├── jars/
├── scripts/
│   ├── configs/
│   │   └── silver_table_config.py
│   ├── ingestion/
│   │   ├── 01_ingest_branches_to_bronze.py
│   │   └── 02_postgres_to_bronze_full_load.py
│   ├── transformations/
│   │   ├── run_silver_pipeline.py
│   │   └── earlier individual silver scripts
│   └── utils/
│       ├── config.py
│       ├── spark_session.py
│       ├── dq_metrics.py
│       └── silver_utils.py
├── notebooks/
├── sql/
└── README.md
```

---

## 5. Environment Configuration

### `.env`

The project uses `.env` to keep connection settings out of code.

Example variables:

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=fleetpulse_db
POSTGRES_SCHEMA=rental_oltp
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password

MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=fleetadmin
MINIO_SECRET_KEY=fleetadmin123
MINIO_BUCKET=fleetpulse-lake
```

---

## 6. Utility Module: `config.py`

### Purpose

Centralizes all project configuration loaded from `.env`.

### Key Responsibilities

- Load environment variables
- Build PostgreSQL JDBC URL
- Build Bronze, Silver, and Gold base paths
- Expose reusable constants to all scripts

### Important Variables

```text
POSTGRES_HOST
POSTGRES_PORT
POSTGRES_DB
POSTGRES_SCHEMA
POSTGRES_USER
POSTGRES_PASSWORD
MINIO_ENDPOINT
MINIO_ACCESS_KEY
MINIO_SECRET_KEY
MINIO_BUCKET
JDBC_URL
BRONZE_BASE_PATH
SILVER_BASE_PATH
GOLD_BASE_PATH
```

### Why This Matters

This avoids hardcoding credentials and paths in every pipeline script.

---

## 7. Utility Module: `spark_session.py`

### Purpose

Creates a reusable Spark session with all required project settings.

### Key Responsibilities

- Start Spark locally
- Attach required JARs
- Configure S3A access to MinIO
- Configure Spark logging
- Provide a single reusable function for all jobs

### Important Spark Concepts Used

```text
SparkSession.builder
appName
master
spark.jars
spark.hadoop.fs.s3a.endpoint
spark.hadoop.fs.s3a.access.key
spark.hadoop.fs.s3a.secret.key
spark.hadoop.fs.s3a.path.style.access
spark.hadoop.fs.s3a.connection.ssl.enabled
spark.hadoop.fs.s3a.impl
spark.driver.host
spark.driver.bindAddress
spark.local.dir
```

### Required JARs

The project uses local JARs instead of resolving packages on every run.

```text
PostgreSQL JDBC Driver
Hadoop AWS / S3A connector
AWS SDK bundle
S3 analytics accelerator dependency
WildFly OpenSSL dependency
Checker Framework dependency
```

### Why This Matters

Spark needs the S3A filesystem implementation to read and write paths like:

```text
s3a://fleetpulse-lake/silver/rental/customers
```

Without the Hadoop AWS/S3A JARs, Spark raises:

```text
ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem
```

---

## 8. Bronze Ingestion Pipeline

### Script

```text
scripts/ingestion/02_postgres_to_bronze_full_load.py
```

### Purpose

Reads all source tables from PostgreSQL using JDBC and writes them to the Bronze layer in MinIO as Parquet.

### Source

```text
PostgreSQL: fleetpulse_db.rental_oltp
```

### Target

```text
s3a://fleetpulse-lake/bronze/rental_oltp/{table_name}
```

### Bronze Tables Loaded

```text
branches
customers
equipment_categories
equipment
rental_contracts
rental_contract_lines
invoices
invoice_lines
payments
maintenance_work_orders
equipment_status_history
```

### Bronze Metadata Columns Added

```text
ingestion_timestamp
ingestion_date
source_table
bronze_load_type
```

### Why These Columns Matter

| Column | Purpose |
|---|---|
| `ingestion_timestamp` | Tracks when the record entered Bronze |
| `ingestion_date` | Supports audit and future partitioning |
| `source_table` | Provides source lineage |
| `bronze_load_type` | Identifies load type, currently `FULL_LOAD` |

### Bronze Validation

The pipeline validates:

```text
source_count == bronze_count
```

If counts do not match, the job raises an exception.

---

## 9. Silver Layer Purpose

The Silver layer converts raw Bronze data into clean, standardized, deduplicated, validated business entities.

Silver responsibilities include:

- Standardizing text fields
- Applying default values
- Deduplicating records
- Creating business record hashes
- Adding audit columns
- Validating required fields
- Validating primary keys
- Applying referential integrity checks
- Producing table-level quality metrics

---

## 10. Utility Module: `dq_metrics.py`

### Class: `DataQualityMetrics`

Tracks data quality and transformation metrics for one Silver table.

### Metrics Tracked

```text
table_name
input_count
after_dedup_count
output_count
duplicates_removed
invalid_records
quarantined_records
null_primary_key_count
duplicate_primary_key_count
pass_rate_pct
status
```

### Methods

#### `calculate_duplicates_removed()`

Calculates how many records were removed by deduplication.

```text
duplicates_removed = input_count - after_dedup_count
```

#### `calculate_invalid_records()`

Calculates how many records were removed from final Silver output.

```text
invalid_records = after_dedup_count - output_count
quarantined_records = invalid_records
```

#### `pass_rate_pct()`

Calculates the percentage of input records that successfully reached Silver output.

```text
pass_rate_pct = output_count / input_count * 100
```

If input count is zero, it returns `0.0` to avoid divide-by-zero errors.

#### `status()`

Returns table-level status.

Current rules:

```text
FAIL if null primary key count > 0
FAIL if duplicate primary key count > 0
PASS if pass rate >= 95
WARN if pass rate >= 80
FAIL otherwise
```

#### `summary()`

Returns all metrics as a Python dictionary.

Example:

```text
{
  'table': 'customers',
  'input_rows': 15,
  'after_dedup_rows': 15,
  'output_rows': 15,
  'duplicates_removed': 0,
  'invalid_records': 0,
  'quarantined_records': 0,
  'null_primary_key_count': 0,
  'duplicate_primary_key_count': 0,
  'pass_rate_pct': 100.0,
  'status': 'PASS'
}
```

---

## 11. Utility Module: `silver_utils.py`

This module contains reusable PySpark functions used by the metadata-driven Silver pipeline.

---

### 11.1 `deduplicate_latest(df, key_columns, order_columns)`

#### Purpose

Keeps the latest record per business key.

#### Functionality Used

```text
Window.partitionBy
orderBy descending
row_number
withColumn
filter
drop
```

#### Logic

```text
1. Partition records by business key columns
2. Order records by latest timestamp columns descending
3. Add row number
4. Keep row_num = 1
5. Drop helper column
```

#### Example Use Case

For `customers`:

```text
Partition by: customer_id
Order by: updated_at DESC, ingestion_timestamp DESC
```

This keeps only the latest version of each customer record.

---

### 11.2 `standardize_columns(df, trim_columns, uppercase_columns)`

#### Purpose

Standardizes string columns by trimming spaces and uppercasing code/status fields.

#### Functionality Used

```text
withColumn
trim
upper
col
```

#### Logic

```text
1. For trim columns, apply trim
2. For uppercase columns, apply trim + upper
```

#### Examples

```text
" active " → "ACTIVE"
" oh "     → "OH"
" cust-001 " → "CUST-001"
```

---

### 11.3 `apply_defaults(df, defaults)`

#### Purpose

Fills null values with configured defaults.

#### Functionality Used

```text
coalesce
lit
withColumn
```

#### Examples

```text
country → USA
customer_status → UNKNOWN
is_deleted → False
payment_amount → 0.00
```

---

### 11.4 `add_record_hash(df, hash_columns, hash_column_name='record_hash')`

#### Purpose

Creates a SHA-256 hash from business columns to support downstream change detection.

#### Functionality Used

```text
sha2
concat_ws
cast('string')
withColumn
```

#### Logic

```text
1. Cast all hash columns to string
2. Concatenate using '|'
3. Apply SHA-256
4. Store in record_hash
```

#### Why This Matters

The `record_hash` allows downstream Gold/SCD logic to detect when a business record has changed.

Example:

```text
same customer_id + different record_hash = customer attributes changed
```

---

### 11.5 `add_silver_audit_columns(df, quality_status='VALID')`

#### Purpose

Adds standard Silver audit columns.

#### Columns Added

```text
silver_created_timestamp
silver_updated_timestamp
silver_quality_status
```

#### Functionality Used

```text
current_timestamp
lit
withColumn
```

#### Quality Status Values

Current values:

```text
VALID
INVALID
```

Future values may include:

```text
WARNING
QUARANTINED
```

---

### 11.6 `validate_primary_key(df, primary_key)`

#### Purpose

Validates primary key quality for a Silver table.

#### Checks Performed

```text
null primary key count
duplicate primary key group count
```

#### Functionality Used

```text
filter
isNull
isNotNull
groupBy
count
```

#### Current Behavior

This function currently supports single-column primary keys.

Example:

```text
customer_id
branch_id
equipment_id
```

Composite key support can be added later.

---

### 11.7 `split_valid_invalid_records(df, required_columns)`

#### Purpose

Splits a DataFrame into valid and invalid records based on required columns.

#### Rule

A record is invalid if any required column is null.

#### Functionality Used

```text
reduce
operator.or_
isNull
filter
```

#### Output

Returns a dictionary:

```text
{
  "valid": valid_df,
  "invalid": invalid_df
}
```

---

### 11.8 `apply_referential_checks(df, spark, referential_checks, silver_base_path)`

#### Purpose

Validates foreign-key relationships against parent Silver tables.

#### Functionality Used

```text
spark.read.parquet
select
distinct
join
drop
```

#### Logic

```text
1. Start with valid child DataFrame
2. For each referential check:
   a. Read parent Silver table
   b. Select distinct parent keys
   c. Inner join child records to parent keys
   d. Keep only records that have matching parent keys
3. Return records that passed all referential checks
```

#### Example

For `rental_contracts`:

```text
customer_id must exist in silver.customers
branch_id must exist in silver.branches
```

For `invoice_lines`:

```text
invoice_id must exist in silver.invoices
contract_line_id must exist in silver.rental_contract_lines
```

---

## 12. Silver Table Configuration

### File

```text
scripts/configs/silver_table_config.py
```

### Purpose

Defines table-specific transformation rules as metadata.

Each table config contains:

```text
table_name
primary_key
trim_columns
uppercase_columns
defaults
hash_columns
required_columns
order_columns
referential_checks
```

### Field Meaning

| Field | Purpose |
|---|---|
| `table_name` | Bronze/Silver folder name |
| `primary_key` | Key used for deduplication and validation |
| `trim_columns` | Columns to apply `trim` only |
| `uppercase_columns` | Columns to apply `trim + upper` |
| `defaults` | Null replacement rules |
| `hash_columns` | Business attributes used for `record_hash` |
| `required_columns` | Columns that must not be null |
| `order_columns` | Columns used to keep latest record |
| `referential_checks` | Parent table validations |

---

## 13. Current Silver Tables and Checks

### 13.1 `branches`

#### Primary Key

```text
branch_id
```

#### Cleansing

- Trim descriptive fields
- Uppercase branch code, state, country, status, source system, load type
- Default `country` to `USA`
- Default `branch_status` to `UNKNOWN`
- Default `is_deleted` to `False`

#### Required Columns

```text
branch_id
branch_code
branch_name
```

#### Hash Tracks

```text
branch_code
branch_name
region
district
city
state
country
postal_code
latitude
longitude
branch_status
is_deleted
```

---

### 13.2 `customers`

#### Primary Key

```text
customer_id
```

#### Cleansing

- Trim name, industry, contact, city, source table
- Uppercase customer number, customer type, billing state/country, credit status, customer status, source system, load type
- Default `billing_country` to `USA`
- Default `credit_status` to `UNKNOWN`
- Default `customer_status` to `UNKNOWN`
- Default `is_deleted` to `False`

#### Required Columns

```text
customer_id
customer_number
customer_name
```

#### Hash Tracks

```text
customer_number
customer_name
customer_type
industry
billing_city
billing_state
billing_country
credit_status
customer_status
is_deleted
```

---

### 13.3 `equipment_categories`

#### Primary Key

```text
category_id
```

#### Required Columns

```text
category_id
category_code
category_name
```

#### Hash Tracks

```text
category_code
category_name
parent_category_id
equipment_class
equipment_type
is_deleted
```

---

### 13.4 `equipment`

#### Primary Key

```text
equipment_id
```

#### Required Columns

```text
equipment_id
equipment_number
category_id
owning_branch_id
current_branch_id
```

#### Hash Tracks

```text
equipment_number
serial_number
category_id
owning_branch_id
current_branch_id
manufacturer
model
model_year
acquisition_date
acquisition_cost
equipment_status
telematics_enabled
is_deleted
```

---

### 13.5 `rental_contracts`

#### Primary Key

```text
contract_id
```

#### Required Columns

```text
contract_id
contract_number
customer_id
branch_id
contract_created_date
rental_start_date
```

#### Referential Checks

```text
customer_id → silver.customers.customer_id
branch_id → silver.branches.branch_id
```

---

### 13.6 `rental_contract_lines`

#### Primary Key

```text
contract_line_id
```

#### Required Columns

```text
contract_line_id
contract_id
equipment_id
```

#### Referential Checks

```text
contract_id → silver.rental_contracts.contract_id
equipment_id → silver.equipment.equipment_id
```

---

### 13.7 `invoices`

#### Primary Key

```text
invoice_id
```

#### Required Columns

```text
invoice_id
invoice_number
contract_id
customer_id
branch_id
invoice_date
```

#### Referential Checks

```text
contract_id → silver.rental_contracts.contract_id
customer_id → silver.customers.customer_id
branch_id → silver.branches.branch_id
```

---

### 13.8 `invoice_lines`

#### Primary Key

```text
invoice_line_id
```

#### Required Columns

```text
invoice_line_id
invoice_id
line_description
revenue_type
```

#### Referential Checks

```text
invoice_id → silver.invoices.invoice_id
contract_line_id → silver.rental_contract_lines.contract_line_id
```

---

### 13.9 `payments`

#### Primary Key

```text
payment_id
```

#### Required Columns

```text
payment_id
payment_number
invoice_id
customer_id
payment_date
```

#### Referential Checks

```text
invoice_id → silver.invoices.invoice_id
customer_id → silver.customers.customer_id
```

---

### 13.10 `maintenance_work_orders`

#### Primary Key

```text
work_order_id
```

#### Required Columns

```text
work_order_id
work_order_number
equipment_id
branch_id
opened_date
```

#### Referential Checks

```text
equipment_id → silver.equipment.equipment_id
branch_id → silver.branches.branch_id
```

---

### 13.11 `equipment_status_history`

#### Primary Key

```text
status_history_id
```

#### Required Columns

```text
status_history_id
equipment_id
branch_id
new_status
status_start_timestamp
```

#### Referential Checks

```text
equipment_id → silver.equipment.equipment_id
branch_id → silver.branches.branch_id
```

---

## 14. Silver Pipeline Runner

### Script

```text
scripts/transformations/run_silver_pipeline.py
```

### Purpose

Processes all configured Silver tables using reusable utilities and metadata-driven table configuration.

### Processing Flow

```text
1. Start Spark once
2. Loop through configured tables
3. Read Bronze Parquet
4. Count input records
5. Standardize columns
6. Apply default values
7. Deduplicate latest records
8. Calculate duplicates removed
9. Add record hash
10. Split valid and invalid records based on required columns
11. Apply referential integrity checks when configured
12. Add Silver audit columns
13. Validate primary key
14. Write valid records to Silver
15. Calculate output metrics
16. Print final summary
17. Stop Spark
```

### Current Tables Processed

```text
branches
customers
equipment_categories
equipment
rental_contracts
rental_contract_lines
invoices
invoice_lines
payments
maintenance_work_orders
equipment_status_history
```

### Example Output

```text
[PASS] branches: input=4, output=4, dedup=0, quarantined=0, pass_rate=100.0%
[PASS] customers: input=15, output=15, dedup=0, quarantined=0, pass_rate=100.0%
[PASS] equipment_status_history: input=18, output=18, dedup=0, quarantined=0, pass_rate=100.0%
```

---

## 15. Data Cleansing Standards

### Trim Only

Used for descriptive or human-readable text fields.

Examples:

```text
customer_name
branch_name
manufacturer
model
line_description
issue_description
```

### Trim + Uppercase

Used for codes, statuses, identifiers, and system values.

Examples:

```text
customer_number
branch_code
equipment_number
contract_status
payment_status
source_system
bronze_load_type
```

### Default Values

Used to avoid nulls in important standard fields.

Examples:

```text
country → USA
status fields → UNKNOWN
amounts → 0.00
quantities → 0 or 1 depending on business meaning
is_deleted → False
```

### Boolean Defaults

Boolean fields are defaulted using boolean values, not strings.

Correct:

```text
False
```

Avoid:

```text
"false"
```

---

## 16. Validation Checks Implemented

### 16.1 Row Count Tracking

Tracks:

```text
input_count
after_dedup_count
output_count
```

### 16.2 Duplicate Removal Tracking

Tracks:

```text
duplicates_removed = input_count - after_dedup_count
```

### 16.3 Required Column Validation

Invalid if any required column is null.

Examples:

```text
customer_id is null
customer_number is null
contract_id is null
invoice_id is null
```

### 16.4 Primary Key Validation

Checks:

```text
null primary key count
duplicate primary key count
```

### 16.5 Referential Integrity Validation

Checks whether child keys exist in parent Silver tables.

Examples:

```text
rental_contracts.customer_id exists in customers
rental_contract_lines.equipment_id exists in equipment
payments.invoice_id exists in invoices
```

### 16.6 Pass Rate Calculation

```text
pass_rate_pct = output_count / input_count * 100
```

### 16.7 Table Status

```text
PASS
WARN
FAIL
```

Current status rules:

```text
FAIL if null primary key count > 0
FAIL if duplicate primary key count > 0
PASS if pass rate >= 95%
WARN if pass rate >= 80%
FAIL otherwise
```

---

## 17. Downstream Change Tracking Columns

The Silver pipeline creates columns that downstream Gold jobs can use for change detection, audit, and lineage.

### `record_hash`

Tracks business attribute changes.

Use case:

```text
same natural key + different record_hash = changed business record
```

Useful for:

```text
SCD Type 2 dimensions
CDC-style comparisons
change detection
incremental processing
```

### `silver_created_timestamp`

Records when the Silver version of the row was created.

### `silver_updated_timestamp`

Records when the Silver row was processed or refreshed.

### `silver_quality_status`

Tracks the quality state of the record.

Current values:

```text
VALID
INVALID
```

### Bronze lineage columns preserved

```text
ingestion_timestamp
ingestion_date
source_table
bronze_load_type
source_system
```

These allow downstream users to trace the data back to the original source and load cycle.

---

## 18. Current Known Local Development Notes

### SLF4J Warning

A warning may appear:

```text
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder"
```

This warning does not currently block the pipeline.

### Windows Spark Cleanup Warnings

Spark may sometimes log temporary directory cleanup warnings on Windows when local JAR files are still locked by the JVM.

This usually happens after successful pipeline completion and does not indicate data failure if the pipeline summary shows `PASS`.

---

## 19. Completed Milestones

```text
1. Created PostgreSQL OLTP source schema
2. Loaded realistic seed data
3. Created MinIO S3-compatible bucket
4. Created Bronze/Silver/Gold data lake paths
5. Built PostgreSQL-to-Bronze full-load pipeline
6. Added Bronze metadata columns
7. Built reusable Spark session utility
8. Built reusable config utility
9. Built DataQualityMetrics class
10. Built reusable Silver utilities
11. Built metadata-driven Silver table config
12. Built generic Silver pipeline runner
13. Processed all 11 source tables into Silver
14. Added data quality summary output
15. Added referential integrity checks for dependent tables
```

---

## 20. Next Planned Stage: Gold Layer

The next phase is to build dimensional Gold models.

Expected Gold dimensions:

```text
dim_customer
dim_branch
dim_equipment_category
dim_equipment
dim_date
```

Expected Gold facts:

```text
fact_rental_contract_line
fact_invoice
fact_payment
fact_maintenance_work_order
fact_equipment_status_history
```

Gold will use Silver tables as cleaned, trusted inputs.

---

## 21. Interview Explanation

A concise explanation of the current pipeline:

> I built a local data engineering pipeline where PostgreSQL simulates an OLTP rental source system and MinIO simulates S3-compatible object storage. I implemented a PySpark Bronze ingestion pipeline that reads PostgreSQL tables through JDBC and writes raw Parquet files to the Bronze layer. Then I built a metadata-driven Silver pipeline with reusable utilities for standardization, defaults, deduplication, record hashing, required-field validation, primary-key validation, referential integrity checks, Silver audit columns, and table-level data quality metrics. The Silver runner processes all 11 source tables through one framework and produces a final quality summary for each table.

---

## 22. Common Commands

### Activate Virtual Environment

```powershell
.\.venv\Scripts\Activate.ps1
```

### Set Python Path

```powershell
$env:PYTHONPATH = (Get-Location).Path
```

### Run Bronze Full Load

```powershell
python .\scripts\ingestion\02_postgres_to_bronze_full_load.py
```

### Run Silver Pipeline

```powershell
python .\scripts\transformations\run_silver_pipeline.py
```

### Run Utility Tests

```powershell
python .\scripts\utils\test_dq_metrics.py
python .\scripts\utils\test_silver_utils.py
```
