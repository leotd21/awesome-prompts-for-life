# ExecuteSQL vs ExecuteSQLRecord: Column Name Handling

## ExecuteSQL Processor (Your Current Approach)

### Configuration
```
Processor: ExecuteSQL
- Database Connection Pool: [Your DB Pool]
- SQL Query: SELECT * FROM source.dbo.src_table
```

### Internal Process Flow
```
SQL Query → JDBC ResultSet → Automatic Avro Conversion → FlowFile
```

### Column Name Transformation
| Original Column | JDBC ResultSet | Avro FlowFile | Status |
|----------------|----------------|---------------|---------|
| `[Nhóm_B]` | `Nhóm_B` | `Nh_m_B` | ❌ Changed |
| `[B_T-1]` | `B_T-1` | `B_T_1` | ❌ Changed |
| `[Normal_Col]` | `Normal_Col` | `Normal_Col` | ✅ OK |

### Key Points
- ❌ **Always outputs Avro format** (no configuration option)
- ❌ **Automatically sanitizes column names** to Avro-compliant format
- ❌ **No control over output format**
- ✅ Simple to use
- ✅ Good performance

---

## ExecuteSQLRecord Processor (Better Alternative)

### Configuration
```
Processor: ExecuteSQLRecord
- Database Connection Pool: [Your DB Pool]
- SQL Query: SELECT * FROM source.dbo.src_table
- Record Writer: JsonRecordSetWriter (or CSV, Parquet)
```

### Internal Process Flow
```
SQL Query → JDBC ResultSet → Record Writer (You Choose!) → FlowFile
```

### Column Name Transformation (with JsonRecordSetWriter)
| Original Column | JDBC ResultSet | JSON FlowFile | Status |
|----------------|----------------|---------------|---------|
| `[Nhóm_B]` | `Nhóm_B` | `Nhóm_B` | ✅ Preserved |
| `[B_T-1]` | `B_T-1` | `B_T-1` | ✅ Preserved |
| `[Normal_Col]` | `Normal_Col` | `Normal_Col` | ✅ OK |

### Key Points
- ✅ **You control the output format** via Record Writer
- ✅ **Can preserve special characters** using JSON/CSV writers
- ✅ **More flexible** for different use cases
- ⚠️ Slightly more complex configuration
- ⚠️ May have minor performance differences

---

## Recommended Solution for Your Case

### Flow Design
```
ExecuteSQLRecord → PutDatabaseRecord
(with JsonWriter)   (with JsonReader)
```

### Step 1: ExecuteSQLRecord Configuration
```yaml
Processor: ExecuteSQLRecord
Properties:
  - Database Connection Pool: SQLServer-Source
  - SQL Query: SELECT * FROM source.dbo.src_table
  - Record Writer: JsonRecordSetWriter
```

### Step 2: JsonRecordSetWriter Controller Service
```yaml
Controller Service: JsonRecordSetWriter
Properties:
  - Schema Write Strategy: Do Not Write Schema
  - Schema Access Strategy: Inherit Record Schema
  - Pretty Print JSON: false
  - Suppress Null Values: Never Suppress
```

### Step 3: PutDatabaseRecord Configuration
```yaml
Processor: PutDatabaseRecord
Properties:
  - Record Reader: JsonTreeReader
  - Database Connection Pool: SQLServer-Destination
  - Statement Type: INSERT
  - Table Name: destination.dbo.dest_table
  - Translate Field Names: false
  - Quote Column Identifiers: true ⚠️ CRITICAL!
  - Quote Table Identifiers: true
```

### Step 4: JsonTreeReader Controller Service
```yaml
Controller Service: JsonTreeReader
Properties:
  - Schema Access Strategy: Infer Schema
```

---

## Why This Works

### JSON Preserves Column Names
JSON format naturally supports:
- ✅ Unicode characters (Vietnamese: Nhóm, ô, etc.)
- ✅ Hyphens and special characters
- ✅ Spaces (if needed)
- ✅ Any UTF-8 character

Example JSON output:
```json
[
  {
    "Nhóm_B": "Value1",
    "B_T-1": 123,
    "Normal_Col": "Data"
  }
]
```

### SQL Server Column Quoting
When PutDatabaseRecord generates the INSERT statement with `Quote Column Identifiers = true`:

```sql
INSERT INTO [destination].[dbo].[dest_table] 
  ([Nhóm_B], [B_T-1], [Normal_Col]) 
VALUES (?, ?, ?)
```

The square brackets `[]` allow SQL Server to accept special characters.

---

## Performance Comparison

| Aspect | ExecuteSQL (Avro) | ExecuteSQLRecord (JSON) | Winner |
|--------|-------------------|-------------------------|---------|
| Binary Size | Smaller | Larger | Avro |
| Processing Speed | Faster | Slightly Slower | Avro |
| Column Name Support | Limited | Full | JSON |
| Human Readable | No | Yes | JSON |
| Debugging | Harder | Easier | JSON |
| **Your Use Case** | ❌ Fails | ✅ Works | **JSON** |

---

## Alternative: CSV Format

If you prefer CSV over JSON:

### ExecuteSQLRecord with CSV
```yaml
Record Writer: CSVRecordSetWriter
Properties:
  - Schema Write Strategy: Set 'schema.name' Attribute
  - Schema Access Strategy: Inherit Record Schema
  - Include Header Line: true
  - Quote Mode: ALL (Important!)
```

### PutDatabaseRecord with CSV
```yaml
Record Reader: CSVReader
Properties:
  - Schema Access Strategy: Use String Fields From Header
  - Treat First Line as Header: true
```

**CSV Pros:**
- ✅ Preserves special characters
- ✅ Very simple format
- ✅ Easy to debug/inspect

**CSV Cons:**
- ⚠️ Larger file size
- ⚠️ Type information may be lost
- ⚠️ NULL handling can be tricky

---

## Testing Steps

### 1. Inspect FlowFile Content
Add **LogAttribute** and **LogMessage** processors:

```
ExecuteSQLRecord → LogAttribute → LogMessage → PutDatabaseRecord
```

LogMessage configuration:
- Message: `${literal('FlowFile content: ')}${literal('${content}')}` (up to 100KB)

### 2. Check Column Names in FlowFile
Look at the FlowFile attributes:
- `record.count` - number of records
- `schema.name` - schema identifier
- Content - actual data with column names

### 3. Validate Destination Data
```sql
-- Check if data inserted correctly
SELECT TOP 10 
    [Nhóm_B],
    [B_T-1],
    *
FROM destination.dbo.dest_table

-- Compare with source
SELECT 
    CASE 
        WHEN src.[Nhóm_B] = dst.[Nhóm_B] THEN 'Match'
        ELSE 'Mismatch'
    END AS Nhom_B_Check
FROM source.dbo.src_table src
JOIN destination.dbo.dest_table dst ON src.ID = dst.ID
```

---

## Summary

**Your observation is 100% correct:**
- ExecuteSQL **always** converts to Avro
- Avro **automatically sanitizes** column names
- This causes the column name mismatch

**Solution:**
- Use **ExecuteSQLRecord** instead of ExecuteSQL
- Configure it with **JsonRecordSetWriter** (or CSVRecordSetWriter)
- JSON/CSV formats **preserve** special characters
- Configure PutDatabaseRecord with **Quote Column Identifiers = true**

This approach completely avoids the Avro conversion problem!
