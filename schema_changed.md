# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Ingestion - Handle Schema Evolution
# MAGIC 
# MAGIC This notebook ingests data from a Databricks Volume source with inconsistent schema across partitions.
# MAGIC Handles multiple columns with data type changes across partitions.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------
from pyspark.sql.types import *

# Configuration parameters
SOURCE_PATH = "/Volumes/catalog/schema/volume_name"
TARGET_TABLE = "catalog.schema.bronze_table_name"
PARTITION_COLUMN = "report_date"

# Define target schema for columns with type conflicts
# Map column names to their target data types
TARGET_SCHEMA_MAPPING = {
    "biz_date": DateType(),
    "amount": DecimalType(18, 2),
    "transaction_date": TimestampType(),
    "customer_id": StringType(),
    "is_active": BooleanType(),
    # Add more columns as needed
}

# Define date format patterns for string-to-date conversions
DATE_FORMATS = {
    "biz_date": "yyyy-MM-dd",
    "transaction_date": "yyyy-MM-dd HH:mm:ss"
}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: Discover Partitions and Their Schemas

# COMMAND ----------
from pyspark.sql.functions import col, to_date, to_timestamp, lit, input_file_name, current_timestamp
import re
from collections import defaultdict

def get_partitions(base_path, partition_col):
    """Get list of partition values from the directory structure"""
    try:
        files = dbutils.fs.ls(base_path)
        partitions = []
        
        for file in files:
            if file.isDir() and f"{partition_col}=" in file.name:
                match = re.search(f"{partition_col}=([^/]+)", file.name)
                if match:
                    partitions.append(match.group(1))
        
        return sorted(partitions)
    except Exception as e:
        print(f"Error reading partitions: {e}")
        return []

partitions = get_partitions(SOURCE_PATH, PARTITION_COLUMN)
print(f"Found {len(partitions)} partitions")
if partitions:
    print(f"First partition: {partitions[0]}")
    print(f"Last partition: {partitions[-1]}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: Analyze Schema Evolution Across All Partitions

# COMMAND ----------
def analyze_partition_schema(base_path, partition_col, partition_value):
    """Get the complete schema of a partition"""
    try:
        partition_path = f"{base_path}/{partition_col}={partition_value}"
        df_sample = spark.read.parquet(partition_path).limit(1)
        
        schema_info = {}
        for field in df_sample.schema.fields:
            schema_info[field.name] = {
                'type': str(field.dataType),
                'nullable': field.nullable
            }
        return schema_info
    except Exception as e:
        return {'error': str(e)}

def detect_schema_changes(partitions, base_path, partition_col, sample_size=None):
    """
    Analyze schema changes across partitions
    Returns a dictionary of column names and their data types per partition
    """
    schema_evolution = defaultdict(lambda: defaultdict(list))
    
    # Sample partitions if there are too many
    partitions_to_check = partitions
    if sample_size and len(partitions) > sample_size:
        step = len(partitions) // sample_size
        partitions_to_check = partitions[::step]
        print(f"Sampling {len(partitions_to_check)} partitions out of {len(partitions)}")
    
    print("Analyzing schema evolution...")
    for i, partition in enumerate(partitions_to_check):
        if i % 10 == 0:
            print(f"  Progress: {i}/{len(partitions_to_check)}")
        
        schema = analyze_partition_schema(base_path, partition_col, partition)
        if 'error' not in schema:
            for col_name, col_info in schema.items():
                col_type = col_info['type']
                if partition not in schema_evolution[col_name][col_type]:
                    schema_evolution[col_name][col_type].append(partition)
    
    return schema_evolution

# Analyze schema changes (sample first 20 partitions for speed)
schema_evolution = detect_schema_changes(partitions, SOURCE_PATH, PARTITION_COLUMN, sample_size=20)

# COMMAND ----------
# Display schema evolution report
print("=" * 80)
print("SCHEMA EVOLUTION REPORT")
print("=" * 80)

columns_with_changes = {}
for col_name, type_dict in schema_evolution.items():
    if len(type_dict) > 1:  # Column has multiple data types
        columns_with_changes[col_name] = type_dict
        print(f"\n⚠️  Column: {col_name}")
        print(f"   Found {len(type_dict)} different data types:")
        for dtype, partition_list in type_dict.items():
            print(f"     • {dtype}: {len(partition_list)} partition(s)")
            print(f"       Example partitions: {partition_list[:3]}")

if not columns_with_changes:
    print("\n✓ No schema evolution detected across sampled partitions")
else:
    print(f"\n\nTotal columns with type changes: {len(columns_with_changes)}")
    print(f"Columns: {list(columns_with_changes.keys())}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: Define Type Conversion Functions

# COMMAND ----------
def get_type_category(spark_type):
    """Categorize Spark data types for conversion logic"""
    if isinstance(spark_type, (StringType,)):
        return "string"
    elif isinstance(spark_type, (DateType,)):
        return "date"
    elif isinstance(spark_type, (TimestampType,)):
        return "timestamp"
    elif isinstance(spark_type, (IntegerType, LongType, ShortType, ByteType)):
        return "integer"
    elif isinstance(spark_type, (FloatType, DoubleType, DecimalType)):
        return "decimal"
    elif isinstance(spark_type, (BooleanType,)):
        return "boolean"
    else:
        return "other"

def convert_column_to_target_type(df, column_name, current_type, target_type):
    """
    Convert a column from current_type to target_type with appropriate logic
    """
    current_category = get_type_category(current_type)
    target_category = get_type_category(target_type)
    
    # If types match, no conversion needed
    if current_category == target_category:
        if isinstance(target_type, DecimalType) and not isinstance(current_type, DecimalType):
            # Special case: need to cast to specific decimal precision
            return df.withColumn(column_name, col(column_name).cast(target_type))
        return df
    
    # Conversion logic based on target type
    if target_category == "date":
        if current_category == "string":
            date_format = DATE_FORMATS.get(column_name, "yyyy-MM-dd")
            return df.withColumn(column_name, to_date(col(column_name), date_format))
        elif current_category == "timestamp":
            return df.withColumn(column_name, col(column_name).cast(DateType()))
        else:
            # Try string intermediate conversion
            return df.withColumn(column_name, to_date(col(column_name).cast("string")))
    
    elif target_category == "timestamp":
        if current_category == "string":
            ts_format = DATE_FORMATS.get(column_name, "yyyy-MM-dd HH:mm:ss")
            return df.withColumn(column_name, to_timestamp(col(column_name), ts_format))
        elif current_category == "date":
            return df.withColumn(column_name, col(column_name).cast(TimestampType()))
        else:
            return df.withColumn(column_name, to_timestamp(col(column_name).cast("string")))
    
    elif target_category == "decimal":
        # Convert to decimal (handles integers, floats, strings)
        return df.withColumn(column_name, col(column_name).cast(target_type))
    
    elif target_category == "integer":
        # Convert to integer
        return df.withColumn(column_name, col(column_name).cast(target_type))
    
    elif target_category == "boolean":
        if current_category == "string":
            # Handle common string representations of boolean
            from pyspark.sql.functions import when, lower, trim
            return df.withColumn(
                column_name,
                when(lower(trim(col(column_name))).isin("true", "1", "yes", "y", "t"), True)
                .when(lower(trim(col(column_name))).isin("false", "0", "no", "n", "f"), False)
                .otherwise(None)
            )
        else:
            return df.withColumn(column_name, col(column_name).cast(BooleanType()))
    
    elif target_category == "string":
        # Converting to string is straightforward
        return df.withColumn(column_name, col(column_name).cast(StringType()))
    
    else:
        # Default: try direct cast
        return df.withColumn(column_name, col(column_name).cast(target_type))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4: Read and Standardize Partitions

# COMMAND ----------
def standardize_schema(df, target_schema_mapping):
    """
    Standardize dataframe schema according to target schema mapping
    """
    df_standardized = df
    
    for col_name, target_type in target_schema_mapping.items():
        # Check if column exists in dataframe
        if col_name in df.columns:
            # Get current type
            current_type = [f.dataType for f in df.schema.fields if f.name == col_name][0]
            
            # Convert to target type
            try:
                df_standardized = convert_column_to_target_type(
                    df_standardized, col_name, current_type, target_type
                )
            except Exception as e:
                print(f"Warning: Could not convert {col_name} from {current_type} to {target_type}: {e}")
    
    return df_standardized

def read_and_standardize_partition(base_path, partition_col, partition_value, target_schema_mapping):
    """
    Read a single partition and standardize schema
    """
    partition_path = f"{base_path}/{partition_col}={partition_value}"
    
    try:
        # Read the partition
        df = spark.read.parquet(partition_path)
        
        # Standardize schema
        df_standardized = standardize_schema(df, target_schema_mapping)
        
        # Add partition column back
        df_standardized = df_standardized.withColumn(partition_col, lit(partition_value))
        
        return df_standardized
    
    except Exception as e:
        print(f"Error processing partition {partition_value}: {e}")
        return None

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5: Process All Partitions

# COMMAND ----------
# Process all partitions
dfs = []
failed_partitions = []
partition_stats = []

print(f"Processing {len(partitions)} partitions...")
for i, partition_value in enumerate(partitions):
    if i % 10 == 0:
        print(f"Progress: {i}/{len(partitions)} partitions processed")
    
    df_partition = read_and_standardize_partition(
        SOURCE_PATH, PARTITION_COLUMN, partition_value, TARGET_SCHEMA_MAPPING
    )
    
    if df_partition is not None:
        record_count = df_partition.count()
        dfs.append(df_partition)
        partition_stats.append({
            'partition': partition_value,
            'records': record_count,
            'status': 'success'
        })
    else:
        failed_partitions.append(partition_value)
        partition_stats.append({
            'partition': partition_value,
            'records': 0,
            'status': 'failed'
        })

print(f"\n✓ Successfully processed: {len(dfs)} partitions")
print(f"✗ Failed partitions: {len(failed_partitions)}")

if failed_partitions:
    print(f"\nFailed partition values:")
    for fp in failed_partitions:
        print(f"  • {fp}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 6: Union All Partitions

# COMMAND ----------
if dfs:
    print("Combining all partitions...")
    df_bronze = dfs[0]
    
    for i, df in enumerate(dfs[1:], 1):
        if i % 50 == 0:
            print(f"  Union progress: {i}/{len(dfs)-1}")
        df_bronze = df_bronze.unionByName(df, allowMissingColumns=True)
    
    total_records = df_bronze.count()
    print(f"\n✓ Total records in bronze dataset: {total_records:,}")
    
    # Display sample
    print("\nSample data:")
    display(df_bronze.limit(10))
else:
    raise Exception("No data was successfully processed")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 7: Validate Schema Standardization

# COMMAND ----------
print("=" * 80)
print("SCHEMA VALIDATION REPORT")
print("=" * 80)

bronze_schema = df_bronze.schema
print(f"\nTotal columns: {len(bronze_schema.fields)}")

# Check if target columns have correct types
print("\nTarget column validation:")
validation_results = []

for col_name, target_type in TARGET_SCHEMA_MAPPING.items():
    if col_name in df_bronze.columns:
        actual_type = [f.dataType for f in bronze_schema.fields if f.name == col_name][0]
        is_correct = type(actual_type) == type(target_type)
        status = "✓" if is_correct else "✗"
        
        validation_results.append({
            'column': col_name,
            'expected': str(target_type),
            'actual': str(actual_type),
            'status': 'PASS' if is_correct else 'FAIL'
        })
        
        print(f"{status} {col_name}: Expected {target_type}, Got {actual_type}")
    else:
        print(f"⚠️  {col_name}: Column not found in dataset")
        validation_results.append({
            'column': col_name,
            'expected': str(target_type),
            'actual': 'MISSING',
            'status': 'MISSING'
        })

# Create validation report dataframe
df_validation = spark.createDataFrame(validation_results)
display(df_validation)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 8: Add Metadata Columns

# COMMAND ----------
from pyspark.sql.functions import current_timestamp, lit, sha2, concat_ws

# Add bronze layer metadata
df_bronze_final = (df_bronze
    .withColumn("bronze_ingestion_timestamp", current_timestamp())
    .withColumn("bronze_source_path", lit(SOURCE_PATH))
    .withColumn("bronze_notebook_version", lit("2.0"))
)

# Optional: Add data quality hash for change detection
# Uncomment if you want to track row-level changes
# all_cols = [c for c in df_bronze.columns if c != PARTITION_COLUMN]
# df_bronze_final = df_bronze_final.withColumn(
#     "bronze_row_hash",
#     sha2(concat_ws("||", *all_cols), 256)
# )

print("Final Bronze Schema:")
df_bronze_final.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 9: Write to Bronze Table

# COMMAND ----------
# Write to bronze table (managed table)
print(f"Writing data to {TARGET_TABLE}...")

(df_bronze_final.write
    .mode("overwrite")  # Use "append" for incremental loads
    .format("delta")
    .partitionBy(PARTITION_COLUMN)
    .option("overwriteSchema", "true")  # Allow schema evolution in bronze table
    .saveAsTable(TARGET_TABLE)
)

print(f"✓ Data successfully written to {TARGET_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 10: Verify Bronze Table & Generate Report

# COMMAND ----------
# Verify the bronze table
df_verify = spark.table(TARGET_TABLE)

print("=" * 80)
print("BRONZE TABLE VERIFICATION")
print("=" * 80)

print(f"\nTotal records: {df_verify.count():,}")
print(f"Total partitions: {df_verify.select(PARTITION_COLUMN).distinct().count()}")

print("\nBronze table schema:")
df_verify.printSchema()

# Partition-level statistics
print("\nRecords per partition:")
df_partition_counts = (df_verify
    .groupBy(PARTITION_COLUMN)
    .count()
    .orderBy(PARTITION_COLUMN)
)
display(df_partition_counts)

# Data quality checks
print("\nData quality checks for target columns:")
for col_name in TARGET_SCHEMA_MAPPING.keys():
    if col_name in df_verify.columns:
        null_count = df_verify.filter(col(col_name).isNull()).count()
        total = df_verify.count()
        null_pct = (null_count / total * 100) if total > 0 else 0
        print(f"  {col_name}: {null_count:,} nulls ({null_pct:.2f}%)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 11: Create Ingestion Summary Table

# COMMAND ----------
# Create summary statistics
summary_data = [{
    'ingestion_timestamp': current_timestamp(),
    'source_path': SOURCE_PATH,
    'target_table': TARGET_TABLE,
    'total_partitions_processed': len(dfs),
    'total_partitions_failed': len(failed_partitions),
    'total_records': total_records,
    'columns_with_schema_changes': len(columns_with_changes) if columns_with_changes else 0,
    'target_columns_mapped': len(TARGET_SCHEMA_MAPPING)
}]

df_summary = spark.createDataFrame(summary_data)
display(df_summary)

# Optional: Save to audit table
# df_summary.write.mode("append").saveAsTable("catalog.schema.bronze_ingestion_audit")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Incremental Load Functions

# COMMAND ----------
def get_existing_partitions(table_name, partition_col):
    """Get list of partitions already in the bronze table"""
    try:
        df = spark.table(table_name)
        existing = df.select(partition_col).distinct().rdd.flatMap(lambda x: x).collect()
        return set(existing)
    except:
        return set()

def incremental_load(source_path, target_table, partition_col, target_schema_mapping):
    """Load only new partitions"""
    print("Starting incremental load...")
    
    source_partitions = set(get_partitions(source_path, partition_col))
    existing_partitions = get_existing_partitions(target_table, partition_col)
    
    new_partitions = sorted(source_partitions - existing_partitions)
    
    print(f"Source partitions: {len(source_partitions)}")
    print(f"Existing partitions: {len(existing_partitions)}")
    print(f"New partitions to load: {len(new_partitions)}")
    
    if not new_partitions:
        print("✓ No new partitions to load")
        return
    
    print(f"\nNew partitions: {new_partitions[:10]}{'...' if len(new_partitions) > 10 else ''}")
    
    # Process new partitions
    dfs = []
    failed = []
    
    for i, partition_value in enumerate(new_partitions):
        if i % 10 == 0:
            print(f"Progress: {i}/{len(new_partitions)}")
        
        df = read_and_standardize_partition(source_path, partition_col, partition_value, target_schema_mapping)
        if df is not None:
            dfs.append(df)
        else:
            failed.append(partition_value)
    
    if dfs:
        # Union all new partitions
        df_new = dfs[0]
        for df in dfs[1:]:
            df_new = df_new.unionByName(df, allowMissingColumns=True)
        
        # Add metadata
        df_new = (df_new
            .withColumn("bronze_ingestion_timestamp", current_timestamp())
            .withColumn("bronze_source_path", lit(source_path))
            .withColumn("bronze_notebook_version", lit("2.0"))
        )
        
        # Append to bronze table
        (df_new.write
            .mode("append")
            .format("delta")
            .partitionBy(partition_col)
            .saveAsTable(target_table)
        )
        
        print(f"\n✓ Successfully loaded {len(dfs)} new partitions")
        print(f"✗ Failed: {len(failed)} partitions")
        
        if failed:
            print(f"Failed partitions: {failed}")
    else:
        print("✗ No data was successfully processed")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Usage Instructions
# MAGIC 
# MAGIC ### Initial Full Load
# MAGIC Run all cells from top to bottom
# MAGIC 
# MAGIC ### Incremental Load
# MAGIC Run only configuration cells and then:
# MAGIC ```python
# MAGIC incremental_load(SOURCE_PATH, TARGET_TABLE, PARTITION_COLUMN, TARGET_SCHEMA_MAPPING)
# MAGIC ```
# MAGIC 
# MAGIC ### Adding New Columns to Handle
# MAGIC Update the TARGET_SCHEMA_MAPPING dictionary with new columns:
# MAGIC ```python
# MAGIC TARGET_SCHEMA_MAPPING = {
# MAGIC     "biz_date": DateType(),
# MAGIC     "new_column": StringType(),  # Add new columns here
# MAGIC     ...
# MAGIC }
# MAGIC ```
