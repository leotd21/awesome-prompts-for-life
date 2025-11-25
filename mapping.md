## Data Type Mapping Strategy

**No, you should NOT convert everything to strings.** Parquet is a strongly-typed columnar format that supports rich data types. Converting everything to strings would:
- Lose the benefits of Parquet's compression and encoding
- Force downstream users to cast data repeatedly
- Increase storage costs and query times
- Lose data validation at the storage layer

### Recommended Approach: Preserve Native Types

**SQL Server to Parquet Type Mapping:**

```
SQL Server          → Parquet Type
------------------------------------------
BIT                 → BOOLEAN
TINYINT             → INT32
SMALLINT            → INT32
INT                 → INT32
BIGINT              → INT64
DECIMAL/NUMERIC     → DECIMAL(precision, scale)
FLOAT               → DOUBLE
REAL                → FLOAT
MONEY/SMALLMONEY    → DECIMAL(19,4) or DOUBLE
DATE                → DATE (INT32)
DATETIME/DATETIME2  → TIMESTAMP_MILLIS (INT64)
TIME                → TIME_MILLIS (INT32)
VARCHAR/NVARCHAR    → STRING (UTF8)
CHAR/NCHAR          → STRING (UTF8)
BINARY/VARBINARY    → BINARY
UNIQUEIDENTIFIER    → STRING (UTF8)
```

## Apache NiFi Implementation Strategy

### NiFi Flow Design

```
QueryDatabaseTable/GenerateTableFetch
    ↓
ExecuteSQLRecord (with Avro Writer)
    ↓
ConvertAvroToParquet
    ↓
PutS3Object
```

### Key Configuration Points

**1. ExecuteSQLRecord Processor:**
- **Record Writer**: Use `AvroRecordSetWriter`
- **Schema Access Strategy**: Use "Inherit Record Schema"
- This preserves source database metadata including data types

**2. ConvertAvroToParquet Processor:**
- Automatically converts Avro schema to Parquet schema
- Maintains type fidelity from database → Avro → Parquet
- **Compression**: Use SNAPPY or GZIP for balance of speed/size

**3. Schema Registry Strategy:**

Use NiFi's **AvroSchemaRegistry** or **HortonworksSchemaRegistry**:
- Store table schemas centrally
- Version control schema evolution
- Ensure consistency across pipelines

### Example NiFi Configuration

```xml
ExecuteSQLRecord:
- Database Connection Pooling Service: [Your SQL Server Pool]
- SQL Select Query: SELECT * FROM schema.table
- Record Writer: AvroRecordSetWriter
  - Schema Write Strategy: "Embed Avro Schema"
  - Schema Access Strategy: "Inherit Record Schema"
  
ConvertAvroToParquet:
- Compression Type: SNAPPY
- Parquet Writer Properties:
  - parquet.enable.dictionary: true
  - parquet.page.size: 1048576
```

## Handling Special Data Type Scenarios

### Decimal/Numeric Precision
```
Strategy: Preserve exact precision and scale
- SQL Server: DECIMAL(18,4)
- Parquet: DECIMAL(18,4) using INT32/INT64/FIXED_LEN_BYTE_ARRAY
- Never convert to DOUBLE (loses precision)
```

### DateTime and Timezone Considerations
```
Challenge: SQL Server DATETIME doesn't store timezone
Solution:
- DATETIME/DATETIME2 → TIMESTAMP_MILLIS (INT64)
- Store as UTC in Parquet
- Document timezone assumptions in metadata
- Consider DATETIMEOFFSET if timezone info exists
```

### NULL Handling
```
Parquet natively supports NULLs:
- No need for special null markers
- Parquet stores null bitmaps efficiently
- Maintain SQL Server NULL semantics
```

### Large Text/Binary Data
```
TEXT/NTEXT/IMAGE (deprecated types):
- Convert to VARCHAR(MAX)/VARBINARY(MAX) at source
- Store as STRING/BINARY in Parquet
- Consider partitioning if rows >100MB
```

## Downstream Query Strategy

### Option 1: AWS Athena (Recommended)

Create external tables with explicit schema:

```sql
CREATE EXTERNAL TABLE sales_data (
    id INT,
    transaction_date TIMESTAMP,
    amount DECIMAL(18,4),
    customer_name STRING,
    is_active BOOLEAN
)
STORED AS PARQUET
LOCATION 's3://your-bucket/sales_data/'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'classification'='parquet'
);
```

**Benefits:**
- Schema-on-read with type enforcement
- Automatic Parquet metadata reading
- No data movement required

### Option 2: AWS Glue Data Catalog

Use AWS Glue Crawler to automatically:
- Discover Parquet files in S3
- Infer schema from Parquet metadata
- Create/update catalog tables
- Handle schema evolution

```bash
# Glue Crawler Configuration
Crawler name: sql-server-parquet-crawler
Data store: S3
Path: s3://your-bucket/
Schema output: Glue Data Catalog
Frequency: Daily or on-demand
```

### Option 3: Spark/PySpark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("S3Read").getOrCreate()

# Parquet automatically reads schema with correct types
df = spark.read.parquet("s3://your-bucket/sales_data/")

# Schema is preserved automatically
df.printSchema()
# root
#  |-- id: integer
#  |-- transaction_date: timestamp
#  |-- amount: decimal(18,4)
#  |-- customer_name: string
#  |-- is_active: boolean
```

## Best Practices & Recommendations

### 1. **Partitioning Strategy**
```
Organize data by date for query performance:
s3://bucket/table_name/year=2024/month=11/day=26/data.parquet

NiFi Expression Language:
${now():format('yyyy')}/${now():format('MM')}/${now():format('dd')}
```

### 2. **Schema Evolution Handling**
- Use Parquet schema evolution features
- Store schema version in S3 metadata
- Implement backward compatibility checks
- Document breaking changes

### 3. **Data Validation Pipeline**
```
QueryDatabase → ValidateRecord → ConvertToParquet → PutS3

Add ValidateRecord processor:
- Verify data types match expected schema
- Check for NULL violations
- Validate ranges for numeric types
- Log validation failures separately
```

### 4. **Metadata Management**
Store alongside Parquet files:
```json
{
  "source_system": "SQL_Server_2016",
  "source_database": "ProductionDB",
  "source_table": "Sales",
  "extraction_timestamp": "2024-11-26T10:30:00Z",
  "row_count": 1500000,
  "schema_version": "v1.2",
  "data_types_mapping": {
    "id": "INT → INT32",
    "amount": "DECIMAL(18,4) → DECIMAL(18,4)"
  }
}
```

### 5. **Performance Optimization**
- **Row Group Size**: 128MB default (good balance)
- **Page Size**: 1MB default
- **Dictionary Encoding**: Enable for string columns
- **Compression**: SNAPPY for general use, GZIP for archival

## Testing & Validation Checklist

1. **Type Preservation Test**
   - Query source SQL Server table
   - Query Parquet in S3 via Athena
   - Compare data types and sample values

2. **Precision Test** (Critical for DECIMAL)
   ```sql
   -- SQL Server
   SELECT SUM(amount) FROM sales;
   
   -- Athena
   SELECT SUM(amount) FROM s3_sales;
   -- Results should match exactly
   ```

3. **NULL Handling Test**
   - Verify NULLs are preserved
   - Check NULL counts match

4. **Date/Time Test**
   - Verify timestamp accuracy
   - Check timezone handling

This approach ensures **type-safe, efficient, and queryable data** in S3 without the overhead of string conversion and downstream casting. Downstream users can query data with native types using Athena, Spark, or any Parquet-compatible tool.
