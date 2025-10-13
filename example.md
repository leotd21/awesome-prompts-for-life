# SOLVING DATABRICKS DELTA LAKE DATA QUALITY ISSUES

## **IMMEDIATE ANSWER**
For Problem 1 (duplicates): Use Delta Lake's `MERGE` operation with deduplication logic or `INSERT OVERWRITE` the partition after deduplicating in memory. For Problem 2 (incomplete data): Reprocess the partition using `INSERT OVERWRITE` with dynamic partition replacement after validating the bronze source is complete.

---

## **THE APPROACH**

### **PROBLEM 1: Deduplicating September 2025 Records**

#### **Step 1: Assess the Damage (5-10 minutes)**

First, quantify the duplication to understand the scope:

```python
# Check duplicate count
duplicate_analysis = spark.sql("""
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT primary_key_columns) as unique_records,
        COUNT(*) - COUNT(DISTINCT primary_key_columns) as duplicate_count
    FROM table_silver
    WHERE BUSINESS_DATE BETWEEN '2025-09-01' AND '2025-09-30'
""")
duplicate_analysis.show()

# Identify which records are duplicated and how many times
duplication_detail = spark.sql("""
    SELECT 
        primary_key_col1, 
        primary_key_col2, 
        COUNT(*) as occurrence_count
    FROM table_silver
    WHERE BUSINESS_DATE BETWEEN '2025-09-01' AND '2025-09-30'
    GROUP BY primary_key_col1, primary_key_col2
    HAVING COUNT(*) > 1
    ORDER BY occurrence_count DESC
    LIMIT 100
""")
duplication_detail.show()
```

**Why this matters**: Understanding whether you have 2x duplicates vs. 10x duplicates changes your strategy. You also need to identify which columns constitute a unique record.

#### **Step 2: Choose Your Deduplication Strategy**

You have three viable options:

**Option A: INSERT OVERWRITE (Recommended for large duplicates)**

```python
from pyspark.sql import Window
from pyspark.sql.functions import row_number, col

# Read the duplicated partition
df_duplicated = spark.table("table_silver").filter(
    (col("BUSINESS_DATE") >= "2025-09-01") & 
    (col("BUSINESS_DATE") <= "2025-09-30")
)

# Define your deduplication logic
# Strategy 1: Keep the latest record based on a timestamp
window_spec = Window.partitionBy("primary_key_col1", "primary_key_col2").orderBy(col("insert_timestamp").desc())

df_deduped = df_duplicated.withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

# Validate record count
original_unique_count = df_duplicated.select("primary_key_col1", "primary_key_col2").distinct().count()
deduped_count = df_deduped.count()

print(f"Original unique records: {original_unique_count}")
print(f"Deduplicated records: {deduped_count}")

assert deduped_count == original_unique_count, "Deduplication failed - record count mismatch"

# Atomic replacement of the partition
df_deduped.write \
    .mode("overwrite") \
    .format("delta") \
    .option("replaceWhere", "BUSINESS_DATE >= '2025-09-01' AND BUSINESS_DATE <= '2025-09-30'") \
    .saveAsTable("table_silver")
```

**Option B: MERGE Statement (Better for small duplicates or complex logic)**

```python
# Create a deduplicated temp view
df_duplicated = spark.table("table_silver").filter(
    (col("BUSINESS_DATE") >= "2025-09-01") & 
    (col("BUSINESS_DATE") <= "2025-09-30")
)

window_spec = Window.partitionBy("primary_key_col1", "primary_key_col2").orderBy(col("insert_timestamp").desc())
df_deduped = df_duplicated.withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

df_deduped.createOrReplaceTempView("deduped_202509")

# Use MERGE to atomically replace
spark.sql("""
    MERGE INTO table_silver AS target
    USING deduped_202509 AS source
    ON target.primary_key_col1 = source.primary_key_col1 
       AND target.primary_key_col2 = source.primary_key_col2
       AND target.BUSINESS_DATE = source.BUSINESS_DATE
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# Delete any records that should no longer exist
spark.sql("""
    DELETE FROM table_silver
    WHERE BUSINESS_DATE BETWEEN '2025-09-01' AND '2025-09-30'
      AND (primary_key_col1, primary_key_col2) NOT IN (
          SELECT primary_key_col1, primary_key_col2 FROM deduped_202509
      )
""")
```

**Option C: DELETE + INSERT (Simplest, but brief inconsistency window)**

```python
# First, back up the partition (safety measure)
spark.sql("""
    CREATE OR REPLACE TABLE table_silver_202509_backup AS
    SELECT * FROM table_silver
    WHERE BUSINESS_DATE BETWEEN '2025-09-01' AND '2025-09-30'
""")

# Deduplicate
df_deduped = spark.table("table_silver_202509_backup") \
    .dropDuplicates(["primary_key_col1", "primary_key_col2", "BUSINESS_DATE"])

# Delete and insert in transaction
spark.sql("""
    DELETE FROM table_silver
    WHERE BUSINESS_DATE BETWEEN '2025-09-01' AND '2025-09-30'
""")

df_deduped.write \
    .mode("append") \
    .format("delta") \
    .saveAsTable("table_silver")
```

**Timeframe**: 10-30 minutes depending on partition size

---

### **PROBLEM 2: Backfilling July 2025 with Complete Data**

#### **Step 1: Validate Bronze Layer Completeness (Critical - 10 minutes)**

```python
# Compare record counts and date coverage
bronze_stats = spark.sql("""
    SELECT 
        COUNT(*) as total_records,
        MIN(BUSINESS_DATE) as earliest_date,
        MAX(BUSINESS_DATE) as latest_date,
        COUNT(DISTINCT BUSINESS_DATE) as distinct_days
    FROM table_bronze
    WHERE BUSINESS_DATE BETWEEN '2025-07-01' AND '2025-07-31'
""")

silver_stats = spark.sql("""
    SELECT 
        COUNT(*) as total_records,
        MIN(BUSINESS_DATE) as earliest_date,
        MAX(BUSINESS_DATE) as latest_date,
        COUNT(DISTINCT BUSINESS_DATE) as distinct_days
    FROM table_silver
    WHERE BUSINESS_DATE BETWEEN '2025-07-01' AND '2025-07-31'
""")

print("Bronze Layer Stats:")
bronze_stats.show()
print("Silver Layer Stats:")
silver_stats.show()

# Check for expected business days (assuming 31 days for July)
expected_days = 31  # Adjust based on your business calendar
bronze_days = bronze_stats.select("distinct_days").collect()[0][0]

assert bronze_days == expected_days, f"Bronze layer incomplete: {bronze_days}/{expected_days} days"
```

**Why this matters**: You must verify the bronze layer is truly complete before overwriting silver. Document this validation in your logs.

#### **Step 2: Reprocess with Your Rollup Logic**

```python
# Run your exact rollup transformation logic
# This should be the SAME code from your monthly job
def rollup_bronze_to_silver(start_date, end_date):
    """
    Your existing transformation logic here
    """
    df_bronze = spark.table("table_bronze").filter(
        (col("BUSINESS_DATE") >= start_date) & 
        (col("BUSINESS_DATE") <= end_date)
    )
    
    # Your aggregations/transformations
    df_silver = df_bronze.groupBy("primary_key_col1", "primary_key_col2", "BUSINESS_DATE") \
        .agg(
            # your aggregations here
        )
    
    return df_silver

# Generate the corrected July data
df_july_corrected = rollup_bronze_to_silver("2025-07-01", "2025-07-31")

# Validate row count is reasonable
july_corrected_count = df_july_corrected.count()
july_current_count = spark.table("table_silver") \
    .filter((col("BUSINESS_DATE") >= "2025-07-01") & (col("BUSINESS_DATE") <= "2025-07-31")) \
    .count()

print(f"Current July records: {july_current_count}")
print(f"Corrected July records: {july_corrected_count}")

# Set a threshold - if difference is too large, investigate
percent_change = abs(july_corrected_count - july_current_count) / july_current_count * 100
assert percent_change < 50, f"Record count change too large: {percent_change}%"
```

#### **Step 3: Atomic Partition Replacement**

```python
# Option 1: Using replaceWhere (Recommended - truly atomic)
df_july_corrected.write \
    .mode("overwrite") \
    .format("delta") \
    .option("replaceWhere", "BUSINESS_DATE >= '2025-07-01' AND BUSINESS_DATE <= '2025-07-31'") \
    .option("overwriteSchema", "false") \
    .saveAsTable("table_silver")

# Option 2: Using dynamic partition overwrite
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

df_july_corrected.write \
    .mode("overwrite") \
    .format("delta") \
    .partitionBy("BUSINESS_DATE") \
    .saveAsTable("table_silver")
```

**Timeframe**: 15-45 minutes depending on data volume

---

## **ADVANCED OPTIMIZATION**

### **Production-Grade Safety Framework**

Wrap your operations in a comprehensive safety harness:

```python
import logging
from datetime import datetime
from pyspark.sql.utils import AnalysisException

class PartitionHealer:
    def __init__(self, table_name, partition_col="BUSINESS_DATE"):
        self.table_name = table_name
        self.partition_col = partition_col
        self.logger = logging.getLogger(__name__)
        
    def create_backup(self, start_date, end_date, backup_suffix=None):
        """Create timestamped backup of partition"""
        if backup_suffix is None:
            backup_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        backup_table = f"{self.table_name}_backup_{backup_suffix}"
        
        spark.sql(f"""
            CREATE OR REPLACE TABLE {backup_table} AS
            SELECT * FROM {self.table_name}
            WHERE {self.partition_col} BETWEEN '{start_date}' AND '{end_date}'
        """)
        
        backup_count = spark.table(backup_table).count()
        self.logger.info(f"Backup created: {backup_table} with {backup_count} records")
        
        return backup_table
    
    def validate_replacement(self, df_new, start_date, end_date, tolerance_pct=20):
        """Validate new data before replacement"""
        # Check record count
        current_count = spark.table(self.table_name) \
            .filter(f"{self.partition_col} BETWEEN '{start_date}' AND '{end_date}'") \
            .count()
        
        new_count = df_new.count()
        
        if current_count > 0:
            pct_change = abs(new_count - current_count) / current_count * 100
            if pct_change > tolerance_pct:
                raise ValueError(
                    f"Record count change {pct_change:.2f}% exceeds tolerance {tolerance_pct}%"
                )
        
        # Check schema compatibility
        current_schema = spark.table(self.table_name).schema
        new_schema = df_new.schema
        
        if current_schema != new_schema:
            raise ValueError("Schema mismatch detected")
        
        # Check for nulls in critical columns
        critical_cols = ["primary_key_col1", "primary_key_col2", self.partition_col]
        for col_name in critical_cols:
            null_count = df_new.filter(col(col_name).isNull()).count()
            if null_count > 0:
                raise ValueError(f"Found {null_count} nulls in critical column {col_name}")
        
        self.logger.info("Validation passed")
        return True
    
    def replace_partition_atomic(self, df_new, start_date, end_date):
        """Atomically replace partition with validation"""
        try:
            # 1. Create backup
            backup_table = self.create_backup(start_date, end_date)
            
            # 2. Validate
            self.validate_replacement(df_new, start_date, end_date)
            
            # 3. Replace
            df_new.write \
                .mode("overwrite") \
                .format("delta") \
                .option("replaceWhere", 
                        f"{self.partition_col} >= '{start_date}' AND {self.partition_col} <= '{end_date}'") \
                .saveAsTable(self.table_name)
            
            # 4. Verify
            new_count = spark.table(self.table_name) \
                .filter(f"{self.partition_col} BETWEEN '{start_date}' AND '{end_date}'") \
                .count()
            
            expected_count = df_new.count()
            
            if new_count != expected_count:
                raise ValueError(f"Post-replacement verification failed: {new_count} != {expected_count}")
            
            self.logger.info(f"Successfully replaced partition with {new_count} records")
            
            # 5. Optimize
            spark.sql(f"OPTIMIZE {self.table_name} WHERE {self.partition_col} BETWEEN '{start_date}' AND '{end_date}'")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Partition replacement failed: {str(e)}")
            self.logger.info(f"Restore from backup: {backup_table}")
            raise

# Usage
healer = PartitionHealer("table_silver")

# For Problem 2 (backfill)
df_july_corrected = rollup_bronze_to_silver("2025-07-01", "2025-07-31")
healer.replace_partition_atomic(df_july_corrected, "2025-07-01", "2025-07-31")
```

### **Preventing Future Issues**

**Fix Your Write Pattern** - The root cause is `mode("append")` without deduplication:

```python
# WRONG - Current pattern
df.write.mode("append").partitionBy("BUSINESS_DATE").saveAsTable("table_silver")

# RIGHT - Idempotent pattern
df.write \
    .mode("overwrite") \
    .format("delta") \
    .option("replaceWhere", f"BUSINESS_DATE >= '{start_date}' AND BUSINESS_DATE <= '{end_date}'") \
    .saveAsTable("table_silver")

# BETTER - Use MERGE for true upsert semantics
from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, "table_silver")

delta_table.alias("target").merge(
    df.alias("source"),
    """
    target.primary_key_col1 = source.primary_key_col1 AND
    target.primary_key_col2 = source.primary_key_col2 AND
    target.BUSINESS_DATE = source.BUSINESS_DATE
    """
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

**Add Job Idempotency Checks**:

```python
def is_partition_already_processed(table_name, business_date):
    """Check if partition was already successfully processed"""
    try:
        record_count = spark.table(table_name) \
            .filter(col("BUSINESS_DATE") == business_date) \
            .count()
        return record_count > 0
    except:
        return False

# In your job
business_date = "2025-09-15"

if is_partition_already_processed("table_silver", business_date):
    print(f"Partition {business_date} already processed, skipping...")
    # Either skip or use overwrite mode
else:
    # Process normally
    pass
```

**Implement Data Quality Checks**:

```python
from pyspark.sql.functions import countDistinct

def validate_silver_layer(df, business_date):
    """Run data quality checks before committing"""
    
    checks = []
    
    # Check 1: No duplicates
    total_count = df.count()
    unique_count = df.select("primary_key_col1", "primary_key_col2").distinct().count()
    checks.append(("no_duplicates", total_count == unique_count))
    
    # Check 2: No nulls in key columns
    null_count = df.filter(
        col("primary_key_col1").isNull() | 
        col("primary_key_col2").isNull()
    ).count()
    checks.append(("no_null_keys", null_count == 0))
    
    # Check 3: All records are for correct business date
    date_check = df.filter(col("BUSINESS_DATE") != business_date).count()
    checks.append(("correct_date", date_check == 0))
    
    # Check 4: Record count within expected range (based on historical data)
    expected_min = 10000  # Set based on your data
    expected_max = 50000
    checks.append(("count_in_range", expected_min <= total_count <= expected_max))
    
    # Report
    failed_checks = [name for name, passed in checks if not passed]
    
    if failed_checks:
        raise ValueError(f"Data quality checks failed: {failed_checks}")
    
    return True

# Use in your pipeline
df_new = rollup_bronze_to_silver("2025-09-15", "2025-09-15")
validate_silver_layer(df_new, "2025-09-15")
df_new.write.mode("overwrite")...
```

---

## **TROUBLESHOOTING**

### **Issue: "File not found" during partition overwrite**

**Diagnosis**: The partition doesn't exist yet, or Delta Lake can't locate it.

**Solution**: Use `mode("append")` for the first write, then switch to `replaceWhere` for subsequent updates. Or check if partition exists first:

```python
partition_exists = spark.sql(f"""
    SELECT COUNT(*) as cnt FROM table_silver 
    WHERE BUSINESS_DATE BETWEEN '{start_date}' AND '{end_date}'
""").collect()[0][0] > 0

mode = "overwrite" if partition_exists else "append"
```

### **Issue: Downstream queries returning inconsistent results during replacement**

**Diagnosis**: Readers are catching the table mid-update.

**Solution**: Delta Lake provides snapshot isolation, but you can add explicit versioning:

```python
# Before replacement, note current version
current_version = spark.sql("DESCRIBE HISTORY table_silver LIMIT 1").select("version").collect()[0][0]

# After replacement
new_version = spark.sql("DESCRIBE HISTORY table_silver LIMIT 1").select("version").collect()[0][0]

# Downstream consumers can read specific versions
df = spark.read.format("delta").option("versionAsOf", current_version).table("table_silver")
```

### **Issue: MERGE operation is too slow**

**Diagnosis**: Large partition sizes or unoptimized Delta table.

**Solution**: 

```python
# 1. Optimize table first
spark.sql("OPTIMIZE table_silver WHERE BUSINESS_DATE BETWEEN '2025-09-01' AND '2025-09-30'")

# 2. Z-order by join keys
spark.sql("OPTIMIZE table_silver ZORDER BY (primary_key_col1, primary_key_col2)")

# 3. Increase shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "200")

# 4. Use broadcast join if source is small
df_small.hint("broadcast").createOrReplaceTempView("source_view")
```

### **Issue: Accidentally deleted more data than intended**

**Diagnosis**: Incorrect WHERE clause or predicate in DELETE/replaceWhere.

**Solution**: Delta Lake time travel to the rescue:

```python
# Find version before mistake
spark.sql("DESCRIBE HISTORY table_silver").show(20)

# Restore specific version
spark.sql("RESTORE TABLE table_silver TO VERSION AS OF 42")

# Or restore to timestamp
spark.sql("RESTORE TABLE table_silver TO TIMESTAMP AS OF '2025-09-15T10:30:00'")
```

---

## **PRODUCTION DEPLOYMENT CHECKLIST**

Before running in production:

1. ✅ **Backup created** - Always create backup table before any destructive operation
2. ✅ **Validation passing** - Run data quality checks on new data
3. ✅ **Dry run completed** - Test on development environment with production-like data
4. ✅ **Downtime communicated** - Inform downstream consumers (even though it's brief)
5. ✅ **Rollback plan ready** - Know exact command to restore from backup
6. ✅ **Monitoring in place** - Watch for errors during operation
7. ✅ **Post-deployment verification** - Compare row counts, run sample queries

**Execution order for your specific case:**

```python
# Day 1: Handle September duplicates
healer = PartitionHealer("table_silver")
df_sept_deduped = deduplicate_partition("2025-09-01", "2025-09-30")
healer.replace_partition_atomic(df_sept_deduped, "2025-09-01", "2025-09-30")

# Day 2: Backfill July (after confirming Sept is stable)
df_july_complete = rollup_bronze_to_silver("2025-07-01", "2025-07-31")
healer.replace_partition_atomic(df_july_complete, "2025-07-01", "2025-07-31")

# Day 3: Fix pipeline to prevent future issues
# Update job notebook to use replaceWhere instead of append
```

This approach gives you atomic operations, full rollback capability, and minimal risk to data integrity.
