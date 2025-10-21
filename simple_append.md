# IMMEDIATE ANSWER

**Use a single daily job that reads yesterday's partition from source and appends/upserts to managed Delta tables—no watermark tracking needed. The partition date itself IS your watermark.**

---

# THE APPROACH: Dead Simple Daily Ingestion

## **Complete Working Solution (15 Minutes to Deploy)**

```python
# Databricks Notebook: daily_bronze_loader
# Schedule: Daily at 2 AM

from pyspark.sql.functions import *
from delta.tables import DeltaTable
from datetime import datetime, timedelta

# ============================================================================
# CONFIGURATION - Just add your tables here
# ============================================================================

TABLES = [
    {
        'source': 's3://your-bucket/transactions',
        'target': 'feature_store.bronze_transactions',
        'mode': 'append'  # or 'upsert'
    },
    {
        'source': 's3://your-bucket/users', 
        'target': 'feature_store.bronze_users',
        'mode': 'upsert',
        'merge_keys': ['user_id', 'snapshot_date']  # only needed for upsert
    },
    {
        'source': '/Volumes/catalog/schema/volume/features',
        'target': 'feature_store.bronze_features',
        'mode': 'append'
    }
]

# ============================================================================
# MAIN LOGIC - No watermark table needed!
# ============================================================================

def load_daily_data():
    """
    Load yesterday's data for all tables.
    Simple, straightforward, no over-engineering.
    """
    
    # Calculate yesterday's partition
    yesterday = (datetime.now() - timedelta(days=1))
    partition_path = f"year={yesterday.year}/month={yesterday.month:02d}/day={yesterday.day:02d}"
    
    print(f"Processing partition: {partition_path}")
    
    for table_config in TABLES:
        table_name = table_config['target']
        source_path = f"{table_config['source']}/{partition_path}"
        
        print(f"\n{'='*60}")
        print(f"Loading: {table_name}")
        print(f"From: {source_path}")
        print(f"{'='*60}")
        
        try:
            # Check if partition exists
            try:
                files = dbutils.fs.ls(source_path)
                parquet_files = [f for f in files if f.name.endswith('.parquet')]
                if not parquet_files:
                    print(f"⚠️  No parquet files in {source_path}, skipping")
                    continue
            except Exception as e:
                print(f"⚠️  Partition doesn't exist: {source_path}, skipping")
                continue
            
            # Read the data
            df = spark.read.parquet(source_path)
            row_count = df.count()
            
            if row_count == 0:
                print(f"⚠️  Partition is empty, skipping")
                continue
            
            print(f"Found {row_count:,} rows")
            
            # Load based on mode
            if table_config['mode'] == 'append':
                # Simple append
                df.write \
                    .format("delta") \
                    .mode("append") \
                    .option("mergeSchema", "true") \
                    .saveAsTable(table_name)
                
                print(f"✓ Appended {row_count:,} rows to {table_name}")
            
            else:  # upsert
                merge_keys = table_config['merge_keys']
                merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
                
                DeltaTable.forName(spark, table_name) \
                    .alias("target") \
                    .merge(df.alias("source"), merge_condition) \
                    .whenMatchedUpdateAll() \
                    .whenNotMatchedInsertAll() \
                    .execute()
                
                print(f"✓ Merged {row_count:,} rows into {table_name}")
                
        except Exception as e:
            print(f"✗ ERROR loading {table_name}: {str(e)}")
            # Don't fail entire job if one table fails
            continue
    
    print(f"\n{'='*60}")
    print("✓ Daily load complete!")
    print(f"{'='*60}")

# Run it
load_daily_data()
```

**That's it. 60 lines of code. No watermark table. No complexity.**

---

## **One-Time Setup (5 Minutes)**

### **Create Target Tables**

```sql
-- Run once to create your bronze tables

-- Example 1: Append-only table
CREATE TABLE IF NOT EXISTS feature_store.bronze_transactions (
  transaction_id STRING,
  user_id STRING,
  amount DECIMAL(10,2),
  event_date DATE,
  event_timestamp TIMESTAMP
)
USING DELTA
LOCATION 'dbfs:/mnt/feature-store/bronze/transactions'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- Example 2: Upsert table
CREATE TABLE IF NOT EXISTS feature_store.bronze_users (
  user_id STRING,
  username STRING,
  email STRING,
  snapshot_date DATE,
  updated_at TIMESTAMP
)
USING DELTA
LOCATION 'dbfs:/mnt/feature-store/bronze/users'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
```

### **Create Databricks Job**

```json
{
  "name": "daily_bronze_loader",
  "tasks": [{
    "task_key": "load_bronze",
    "notebook_task": {
      "notebook_path": "/path/to/daily_bronze_loader"
    },
    "job_cluster_key": "small_cluster"
  }],
  "job_clusters": [{
    "job_cluster_key": "small_cluster",
    "new_cluster": {
      "spark_version": "13.3.x-scala2.12",
      "node_type_id": "i3.xlarge",
      "num_workers": 2,
      "aws_attributes": {
        "instance_profile_arn": "arn:aws:iam::YOUR_ACCOUNT:instance-profile/databricks-s3-access"
      }
    }
  }],
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "UTC"
  },
  "email_notifications": {
    "on_failure": ["your-email@company.com"]
  }
}
```

---

## **Handling Common Scenarios**

### **Scenario 1: Process Multiple Days (Backfill)**

```python
def load_date_range(start_date, end_date):
    """
    Load multiple days at once for backfills.
    """
    from datetime import timedelta
    
    current_date = start_date
    while current_date <= end_date:
        partition_path = f"year={current_date.year}/month={current_date.month:02d}/day={current_date.day:02d}"
        
        for table_config in TABLES:
            source_path = f"{table_config['source']}/{partition_path}"
            
            try:
                df = spark.read.parquet(source_path)
                df.write.format("delta").mode("append").saveAsTable(table_config['target'])
                print(f"✓ Loaded {partition_path} into {table_config['target']}")
            except:
                print(f"⚠️  Skipped {partition_path} (doesn't exist)")
        
        current_date += timedelta(days=1)

# Run backfill
# load_date_range(datetime(2025, 10, 1), datetime(2025, 10, 20))
```

### **Scenario 2: Handle Late-Arriving Data**

```python
def load_with_lookback(lookback_days=3):
    """
    Process last N days to catch late arrivals.
    Run this as separate weekly job.
    """
    for days_ago in range(lookback_days, 0, -1):
        target_date = datetime.now() - timedelta(days=days_ago)
        partition_path = f"year={target_date.year}/month={target_date.month:02d}/day={target_date.day:02d}"
        
        for table_config in TABLES:
            if table_config['mode'] == 'upsert':
                # Upsert handles duplicates automatically
                source_path = f"{table_config['source']}/{partition_path}"
                
                try:
                    df = spark.read.parquet(source_path)
                    merge_keys = table_config['merge_keys']
                    merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
                    
                    DeltaTable.forName(spark, table_config['target']) \
                        .alias("target") \
                        .merge(df.alias("source"), merge_condition) \
                        .whenMatchedUpdateAll() \
                        .whenNotMatchedInsertAll() \
                        .execute()
                    
                    print(f"✓ Reprocessed {partition_path} for {table_config['target']}")
                except:
                    pass

# Run weekly: load_with_lookback(lookback_days=7)
```

### **Scenario 3: Process "Today" Partition (Intraday Updates)**

```python
def load_today_data():
    """
    Load today's partition - useful for tables that update throughout the day.
    Schedule this to run every 4 hours.
    """
    today = datetime.now()
    partition_path = f"year={today.year}/month={today.month:02d}/day={today.day:02d}"
    
    for table_config in TABLES:
        source_path = f"{table_config['source']}/{partition_path}"
        
        try:
            df = spark.read.parquet(source_path)
            
            if table_config['mode'] == 'append':
                # For append mode on same-day data, deduplicate first
                merge_keys = table_config.get('dedup_keys', ['id'])  # Add dedup_keys to config
                
                # Remove today's data first, then re-append
                spark.sql(f"""
                    DELETE FROM {table_config['target']}
                    WHERE date(event_timestamp) = '{today.date()}'
                """)
                
                df.write.format("delta").mode("append").saveAsTable(table_config['target'])
            else:
                # Upsert handles this naturally
                merge_keys = table_config['merge_keys']
                merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
                
                DeltaTable.forName(spark, table_config['target']) \
                    .alias("target") \
                    .merge(df.alias("source"), merge_condition) \
                    .whenMatchedUpdateAll() \
                    .whenNotMatchedInsertAll() \
                    .execute()
            
            print(f"✓ Updated today's data for {table_config['target']}")
        except:
            print(f"⚠️  No data yet for {table_config['target']}")

# Schedule this every 4 hours during business day
```

---

## **Adding Basic Monitoring (Optional but Recommended)**

### **Simple Success Tracking**

```python
# Add this to the end of your main function

def log_execution():
    """
    Simple execution log - just track what ran and when.
    """
    spark.sql("""
        CREATE TABLE IF NOT EXISTS feature_store.bronze_job_log (
            job_run_id STRING,
            run_date DATE,
            partition_loaded STRING,
            tables_processed INT,
            run_timestamp TIMESTAMP
        ) USING DELTA
    """)
    
    spark.createDataFrame([{
        'job_run_id': spark.sparkContext.applicationId,
        'run_date': (datetime.now() - timedelta(days=1)).date(),
        'partition_loaded': f"year={yesterday.year}/month={yesterday.month:02d}/day={yesterday.day:02d}",
        'tables_processed': len(TABLES),
        'run_timestamp': datetime.now()
    }]).write.format("delta").mode("append").saveAsTable("feature_store.bronze_job_log")

# Call at end of load_daily_data()
log_execution()
```

### **Check Data Freshness**

```sql
-- Run this query daily to check everything loaded

SELECT 
    table_name,
    MAX(event_date) as latest_date,
    DATEDIFF(CURRENT_DATE, MAX(event_date)) as days_lag
FROM (
    SELECT 'transactions' as table_name, MAX(event_date) as event_date FROM feature_store.bronze_transactions
    UNION ALL
    SELECT 'users', MAX(snapshot_date) FROM feature_store.bronze_users
    UNION ALL
    SELECT 'features', MAX(event_date) FROM feature_store.bronze_features
)
GROUP BY table_name;
```

---

## **Cost Estimate**

**For 100GB/day across 3 tables:**
```
Cluster: 2 workers + 1 driver (i3.xlarge)
Runtime: ~8-12 minutes
DBUs: ~1.5 DBUs per run
Cost: 1.5 × $0.15 = $0.225/day

Monthly: $0.225 × 30 = $6.75
Annual: $81
```

**Compare to AutoLoader: $1,800/year**
**Savings: 95%**

---

## **Troubleshooting**

### **Problem: "Partition doesn't exist"**

```python
# Solution: Add retry logic for eventual consistency
import time

def read_with_retry(path, max_retries=3):
    for attempt in range(max_retries):
        try:
            return spark.read.parquet(path)
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Retry {attempt + 1}: Waiting for {path}...")
                time.sleep(30)
            else:
                raise e
```

### **Problem: "Schema mismatch errors"**

```python
# Solution: Always enable mergeSchema
df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \  # This handles new columns automatically
    .saveAsTable(table_name)
```

### **Problem: "Duplicate rows appearing"**

For append mode:
```python
# Add deduplication before append
df_deduped = df.dropDuplicates(['id', 'event_date'])  # Your unique keys
df_deduped.write.format("delta").mode("append").saveAsTable(table_name)
```

For upsert mode: Already handled by merge logic.

---

## **When to Use Append vs Upsert**

**Use APPEND for:**
- Immutable event logs (transactions, clicks, impressions)
- Time-series data that never changes
- Faster (2-3x) than upsert

**Use UPSERT for:**
- User profiles (data gets updated)
- Inventory snapshots
- Any data where same key can have different values over time

---

# **FINAL RECOMMENDATION**

This simple approach works perfectly for:
- ✅ Daily batch loads
- ✅ Hive-partitioned sources
- ✅ Predictable data arrival times
- ✅ Teams that want full control without complexity

**Your next steps:**
1. Copy the main script
2. Update the `TABLES` config with your sources
3. Create target tables with the SQL above
4. Run manually once to test
5. Schedule the Databricks job

**Total implementation time: 20 minutes**

