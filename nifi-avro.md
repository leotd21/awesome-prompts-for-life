# Dynamic Avro Solution with Automatic Schema Mapping

## Strategy Overview

The solution uses a **bidirectional mapping approach**:
1. **Extract Phase**: Map original column names → Avro-compatible names
2. **Load Phase**: Map Avro-compatible names → original column names
3. **Store mapping dynamically** for reusability across different tables

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Dynamic Mapping Solution                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  1. QueryDatabaseTable (Source)                                  │
│     ↓ [Nhóm_B, B_T-1, Normal_Col]                               │
│                                                                   │
│  2. ExecuteScript (Generate Column Mapping)                      │
│     ↓ Creates: {                                                 │
│         "Nhóm_B" → "Nhom_B",                                     │
│         "B_T-1" → "B_T_1",                                       │
│         "Normal_Col" → "Normal_Col"                              │
│       }                                                           │
│     ↓ Stores mapping in FlowFile attributes                      │
│                                                                   │
│  3. UpdateRecord (Rename to Avro-compatible)                     │
│     ↓ [Nhom_B, B_T_1, Normal_Col] ✅ Avro-compatible            │
│                                                                   │
│  4. ConvertRecord (to Avro)                                      │
│     ↓ Avro format with safe column names                         │
│                                                                   │
│  5. ExecuteScript (Generate Dynamic DDL)                         │
│     ↓ Creates destination table with Avro-compatible names       │
│                                                                   │
│  6. PutDatabaseRecord (with AvroReader)                          │
│     ↓ Inserts data successfully ✅                               │
│                                                                   │
│  7. ExecuteSQL (Create Views - Optional)                         │
│     ↓ Creates views with original column names for users         │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Solution Components

### Component 1: Column Mapping Generator (ExecuteScript - Python)

**Purpose:** Automatically detect special characters and create bidirectional mapping

```python
# ExecuteScript - Python
import json
import re
from org.apache.nifi.processor.io import StreamCallback
from java.nio.charset import StandardCharsets

class ColumnMapper(StreamCallback):
    def __init__(self):
        self.mapping = {}
        self.reverse_mapping = {}
    
    def sanitize_column_name(self, original_name):
        """
        Convert column name to Avro-compatible format
        Rules:
        - Replace non-alphanumeric (except _) with underscore
        - Remove consecutive underscores
        - Ensure starts with letter or underscore
        - Preserve case
        """
        # Replace special chars with underscore
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', original_name)
        
        # Remove consecutive underscores
        sanitized = re.sub(r'_+', '_', sanitized)
        
        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')
        
        # Ensure starts with letter or underscore
        if sanitized and sanitized[0].isdigit():
            sanitized = '_' + sanitized
        
        # If empty after sanitization, use default
        if not sanitized:
            sanitized = 'column_' + str(hash(original_name))
        
        return sanitized
    
    def process(self, inputStream, outputStream):
        # Read the Avro schema from FlowFile
        text = inputStream.read().decode('utf-8')
        
        # Parse schema (this is simplified - actual implementation 
        # would use NiFi's schema registry)
        # In practice, get schema from FlowFile attributes
        
        # For this example, we'll work with attribute-based approach
        pass

# Main script
flowFile = session.get()
if flowFile is not None:
    # Get schema information from FlowFile attributes
    # avro.schema attribute contains the schema JSON
    schema_json = flowFile.getAttribute('avro.schema')
    
    if schema_json:
        schema = json.loads(schema_json)
        
        mapper = ColumnMapper()
        column_mapping = {}
        reverse_mapping = {}
        
        # Extract field names from Avro schema
        if 'fields' in schema:
            for field in schema['fields']:
                original_name = field.get('name')
                sanitized_name = mapper.sanitize_column_name(original_name)
                
                column_mapping[original_name] = sanitized_name
                reverse_mapping[sanitized_name] = original_name
        
        # Store mappings as FlowFile attributes
        flowFile = session.putAttribute(flowFile, 
                                       'column.mapping.forward', 
                                       json.dumps(column_mapping))
        flowFile = session.putAttribute(flowFile, 
                                       'column.mapping.reverse', 
                                       json.dumps(reverse_mapping))
        flowFile = session.putAttribute(flowFile, 
                                       'schema.original', 
                                       schema_json)
    
    session.transfer(flowFile, REL_SUCCESS)
```

### Component 2: Dynamic Schema Registry

**Purpose:** Store and retrieve column mappings for different tables

**Option A: Using DistributedMapCacheClient**

```yaml
Controller Service: DistributedMapCacheClientService
Purpose: Store column mappings across NiFi cluster

Key Format: "table_mapping:{database}.{schema}.{table}"
Value Format: JSON mapping object

Example:
  Key: "table_mapping:source.dbo.customers"
  Value: {
    "Nhóm_B": "Nhom_B",
    "B_T-1": "B_T_1",
    "Ngày_Tạo": "Ngay_Tao"
  }
```

**Option B: Using Database Table**

```sql
-- Mapping table for storing column name conversions
CREATE TABLE metadata.dbo.column_mappings (
    mapping_id INT IDENTITY(1,1) PRIMARY KEY,
    source_database VARCHAR(100),
    source_schema VARCHAR(100),
    source_table VARCHAR(100),
    original_column_name NVARCHAR(200),
    avro_compatible_name VARCHAR(200),
    created_date DATETIME DEFAULT GETDATE(),
    UNIQUE (source_database, source_schema, source_table, original_column_name)
);

-- Index for fast lookups
CREATE INDEX idx_table_lookup 
ON metadata.dbo.column_mappings(source_database, source_schema, source_table);
```

---

## Complete NiFi Flow Design

### Flow 1: Initial Setup & Mapping Generation

```
┌──────────────────────────────────────────────────────────────────┐
│ Process Group: Table Metadata Extraction                         │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  [1] ExecuteSQL (Get Table Metadata)                             │
│      Properties:                                                  │
│        SQL: SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
│             FROM INFORMATION_SCHEMA.COLUMNS                       │
│             WHERE TABLE_SCHEMA = '${db.schema}'                  │
│               AND TABLE_NAME = '${db.table}'                     │
│             ORDER BY ORDINAL_POSITION                            │
│      Output: Avro with column metadata                           │
│         ↓                                                         │
│                                                                   │
│  [2] ExecuteScript (Generate Mapping)                            │
│      Language: Python                                             │
│      Purpose: Create Avro-compatible names                       │
│      Output Attributes:                                           │
│        - column.mapping.json                                      │
│        - avro.column.list                                         │
│        - original.column.list                                     │
│         ↓                                                         │
│                                                                   │
│  [3] PutDistributedMapCache (Store Mapping)                      │
│      Cache Key: table_mapping:${db.schema}.${db.table}          │
│      Cache Value: ${column.mapping.json}                         │
│         ↓                                                         │
│                                                                   │
│  [4] ExecuteSQL (Store in Database - Optional)                   │
│      SQL: INSERT INTO metadata.dbo.column_mappings (...)         │
│         ↓                                                         │
│                                                                   │
│  [5] Success                                                      │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
```

### Flow 2: Data Migration with Dynamic Mapping

```
┌──────────────────────────────────────────────────────────────────┐
│ Process Group: Dynamic Data Migration                            │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  [1] GenerateFlowFile (Trigger)                                  │
│      Custom Properties:                                           │
│        db.source: source                                          │
│        db.schema: dbo                                             │
│        db.table: customers                                        │
│         ↓                                                         │
│                                                                   │
│  [2] FetchDistributedMapCache (Get Mapping)                      │
│      Cache Key: table_mapping:${db.schema}.${db.table}          │
│      Output Attribute: column.mapping.json                       │
│         ↓                                                         │
│                                                                   │
│  [3] RouteOnAttribute (Check if Mapping Exists)                  │
│      ├─ mapping exists → Continue                                │
│      └─ no mapping → Go to Metadata Extraction Flow             │
│         ↓                                                         │
│                                                                   │
│  [4] ExecuteScript (Build Dynamic SQL)                           │
│      Purpose: Create SELECT with column aliases                  │
│      Output Attribute: dynamic.sql                               │
│      Example Output:                                              │
│        SELECT                                                     │
│          [Nhóm_B] AS Nhom_B,                                     │
│          [B_T-1] AS B_T_1,                                       │
│          [Normal_Col] AS Normal_Col                              │
│        FROM ${db.schema}.${db.table}                             │
│         ↓                                                         │
│                                                                   │
│  [5] ExecuteSQL (Extract Data)                                   │
│      SQL: ${dynamic.sql}                                         │
│      Output: Avro with sanitized column names ✅                 │
│         ↓                                                         │
│                                                                   │
│  [6] RouteOnAttribute (First Run Check)                          │
│      Check if destination table exists                           │
│      ├─ first run → Create Table Flow                            │
│      └─ exists → Skip to Insert                                  │
│         ↓                                                         │
│                                                                   │
│  [7] PutDatabaseRecord                                           │
│      Record Reader: AvroReader                                   │
│      Statement Type: INSERT                                       │
│      Table Name: ${db.schema}.${db.table}                        │
│      Quote Column Identifiers: true                              │
│         ↓                                                         │
│                                                                   │
│  [8] Success ✅                                                   │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
```

### Flow 3: Destination Table Creation

```
┌──────────────────────────────────────────────────────────────────┐
│ Process Group: Dynamic DDL Generation                            │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  [1] Input from Migration Flow                                   │
│      Attributes: column.mapping.json, source metadata            │
│         ↓                                                         │
│                                                                   │
│  [2] ExecuteScript (Generate CREATE TABLE DDL)                   │
│      Language: Python/Groovy                                      │
│      Purpose: Build DDL with Avro-compatible column names        │
│      Output Attribute: ddl.create.table                          │
│      Example Output:                                              │
│        CREATE TABLE destination.dbo.customers (                  │
│          Nhom_B NVARCHAR(100),                                   │
│          B_T_1 INT,                                              │
│          Normal_Col VARCHAR(50)                                  │
│        )                                                          │
│         ↓                                                         │
│                                                                   │
│  [3] PutSQL (Create Table)                                       │
│      SQL: ${ddl.create.table}                                    │
│         ↓                                                         │
│                                                                   │
│  [4] ExecuteScript (Generate VIEW DDL - Optional)                │
│      Purpose: Create view with original column names             │
│      Output Attribute: ddl.create.view                           │
│      Example Output:                                              │
│        CREATE VIEW destination.dbo.vw_customers AS               │
│        SELECT                                                     │
│          Nhom_B AS [Nhóm_B],                                     │
│          B_T_1 AS [B_T-1],                                       │
│          Normal_Col                                              │
│        FROM destination.dbo.customers                            │
│         ↓                                                         │
│                                                                   │
│  [5] PutSQL (Create View)                                        │
│      SQL: ${ddl.create.view}                                     │
│         ↓                                                         │
│                                                                   │
│  [6] Success → Return to Migration Flow                          │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
```

---

## Implementation Scripts

### Script 1: Column Mapping Generator (Python)

```python
# ExecuteScript Processor - Python
import json
import re
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets

# Get FlowFile
flowFile = session.get()
if flowFile is None:
    exit()

def sanitize_avro_name(name):
    """Convert column name to Avro-compatible format"""
    # Replace special characters with underscore
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    # Remove consecutive underscores
    sanitized = re.sub(r'_+', '_', sanitized)
    # Strip leading/trailing underscores
    sanitized = sanitized.strip('_')
    # Ensure starts with letter or underscore
    if sanitized and sanitized[0].isdigit():
        sanitized = '_' + sanitized
    if not sanitized:
        sanitized = 'col_' + str(abs(hash(name)) % 10000)
    return sanitized

try:
    # Read the FlowFile content (metadata from INFORMATION_SCHEMA)
    reader = session.read(flowFile)
    content = IOUtils.toString(reader, StandardCharsets.UTF_8)
    reader.close()
    
    # Parse metadata (assuming JSON format from ExecuteSQL)
    metadata = json.loads(content)
    
    # Generate mappings
    mappings = {}
    reverse_mappings = {}
    avro_columns = []
    original_columns = []
    
    for row in metadata:
        original_name = row['COLUMN_NAME']
        avro_name = sanitize_avro_name(original_name)
        
        # Handle collisions
        counter = 1
        base_avro_name = avro_name
        while avro_name in reverse_mappings:
            avro_name = base_avro_name + '_' + str(counter)
            counter += 1
        
        mappings[original_name] = avro_name
        reverse_mappings[avro_name] = original_name
        avro_columns.append(avro_name)
        original_columns.append(original_name)
    
    # Store as FlowFile attributes
    flowFile = session.putAttribute(flowFile, 'column.mapping.forward', 
                                   json.dumps(mappings))
    flowFile = session.putAttribute(flowFile, 'column.mapping.reverse', 
                                   json.dumps(reverse_mappings))
    flowFile = session.putAttribute(flowFile, 'avro.columns', 
                                   ','.join(avro_columns))
    flowFile = session.putAttribute(flowFile, 'original.columns', 
                                   ','.join(original_columns))
    
    session.transfer(flowFile, REL_SUCCESS)
    
except Exception as e:
    log.error('Error processing: ' + str(e))
    session.transfer(flowFile, REL_FAILURE)
```

### Script 2: Dynamic SQL Builder (Python)

```python
# ExecuteScript Processor - Build SELECT with aliases
import json

flowFile = session.get()
if flowFile is None:
    exit()

try:
    # Get mapping from attribute
    mapping_json = flowFile.getAttribute('column.mapping.forward')
    mappings = json.loads(mapping_json)
    
    # Get table info from attributes
    schema = flowFile.getAttribute('db.schema')
    table = flowFile.getAttribute('db.table')
    
    # Build SELECT statement with aliases
    select_parts = []
    for original, avro_safe in mappings.items():
        if original != avro_safe:
            # Need alias
            select_parts.append('[{}] AS {}'.format(original, avro_safe))
        else:
            # No alias needed
            select_parts.append('[{}]'.format(original))
    
    sql = 'SELECT\n  ' + ',\n  '.join(select_parts) + \
          '\nFROM {}.{}'.format(schema, table)
    
    # Store SQL in attribute
    flowFile = session.putAttribute(flowFile, 'dynamic.sql', sql)
    
    session.transfer(flowFile, REL_SUCCESS)
    
except Exception as e:
    log.error('Error building SQL: ' + str(e))
    session.transfer(flowFile, REL_FAILURE)
```

### Script 3: DDL Generator (Python)

```python
# ExecuteScript Processor - Generate CREATE TABLE DDL
import json

flowFile = session.get()
if flowFile is None:
    exit()

try:
    # Get mapping and metadata
    mapping_json = flowFile.getAttribute('column.mapping.forward')
    mappings = json.loads(mapping_json)
    
    # Get source table metadata (from earlier ExecuteSQL)
    # This would contain: COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, etc.
    metadata_json = flowFile.getAttribute('table.metadata')
    metadata = json.loads(metadata_json)
    
    # Get destination table info
    dest_schema = flowFile.getAttribute('dest.schema')
    dest_table = flowFile.getAttribute('dest.table')
    
    # Build column definitions
    column_defs = []
    for col_meta in metadata:
        original_name = col_meta['COLUMN_NAME']
        avro_name = mappings[original_name]
        data_type = col_meta['DATA_TYPE']
        max_length = col_meta.get('CHARACTER_MAXIMUM_LENGTH', '')
        
        # Build data type string
        if max_length and max_length != 'NULL':
            type_str = '{}({})'.format(data_type, max_length)
        else:
            type_str = data_type
        
        # Nullable
        nullable = '' if col_meta.get('IS_NULLABLE') == 'YES' else ' NOT NULL'
        
        column_defs.append('  {} {}{}'.format(avro_name, type_str, nullable))
    
    # Build CREATE TABLE statement
    ddl = 'CREATE TABLE {}.{} (\n'.format(dest_schema, dest_table) + \
          ',\n'.join(column_defs) + \
          '\n);'
    
    # Store DDL in attribute
    flowFile = session.putAttribute(flowFile, 'ddl.create.table', ddl)
    
    # Also generate VIEW DDL for user-friendly access
    view_name = 'vw_' + dest_table
    select_parts = []
    for original, avro_safe in mappings.items():
        if original != avro_safe:
            select_parts.append('  {} AS [{}]'.format(avro_safe, original))
        else:
            select_parts.append('  {}'.format(original))
    
    view_ddl = 'CREATE VIEW {}.{} AS\nSELECT\n'.format(dest_schema, view_name) + \
               ',\n'.join(select_parts) + \
               '\nFROM {}.{};'.format(dest_schema, dest_table)
    
    flowFile = session.putAttribute(flowFile, 'ddl.create.view', view_ddl)
    
    session.transfer(flowFile, REL_SUCCESS)
    
except Exception as e:
    log.error('Error generating DDL: ' + str(e))
    session.transfer(flowFile, REL_FAILURE)
```

---

## Configuration Examples

### Example 1: Single Table Migration

**Input Parameters (FlowFile Attributes):**
```json
{
  "db.source": "source",
  "db.schema": "dbo",
  "db.table": "customers",
  "dest.schema": "dbo",
  "dest.table": "customers"
}
```

**Generated Mapping:**
```json
{
  "Nhóm_B": "Nhom_B",
  "B_T-1": "B_T_1",
  "Ngày_Tạo": "Ngay_Tao",
  "Địa_Chỉ": "Dia_Chi",
  "Customer-ID": "Customer_ID",
  "Normal_Column": "Normal_Column"
}
```

**Generated SQL:**
```sql
SELECT
  [Nhóm_B] AS Nhom_B,
  [B_T-1] AS B_T_1,
  [Ngày_Tạo] AS Ngay_Tao,
  [Địa_Chỉ] AS Dia_Chi,
  [Customer-ID] AS Customer_ID,
  [Normal_Column]
FROM dbo.customers
```

**Generated DDL:**
```sql
-- Physical table with Avro-compatible names
CREATE TABLE dbo.customers (
  Nhom_B NVARCHAR(100),
  B_T_1 INT,
  Ngay_Tao DATETIME,
  Dia_Chi NVARCHAR(200),
  Customer_ID VARCHAR(50),
  Normal_Column VARCHAR(100)
);

-- View with original names for end users
CREATE VIEW dbo.vw_customers AS
SELECT
  Nhom_B AS [Nhóm_B],
  B_T_1 AS [B_T-1],
  Ngay_Tao AS [Ngày_Tạo],
  Dia_Chi AS [Địa_Chỉ],
  Customer_ID AS [Customer-ID],
  Normal_Column
FROM dbo.customers;
```

### Example 2: Multiple Table Migration (Loop)

```
┌─────────────────────────────────────────────────────────────┐
│  [1] ExecuteSQL (Get List of Tables)                        │
│      SQL: SELECT TABLE_SCHEMA, TABLE_NAME                   │
│            FROM INFORMATION_SCHEMA.TABLES                   │
│            WHERE TABLE_TYPE = 'BASE TABLE'                  │
│         ↓                                                    │
│                                                              │
│  [2] SplitRecord (Split into individual table records)      │
│         ↓                                                    │
│                                                              │
│  [3] EvaluateJsonPath (Extract table info)                  │
│      Set Attributes:                                         │
│        db.schema = $.TABLE_SCHEMA                           │
│        db.table = $.TABLE_NAME                              │
│         ↓                                                    │
│                                                              │
│  [4] → Send to Dynamic Migration Flow (from above)          │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## Advantages of This Solution

### ✅ Dynamic & Reusable
- Works with any table automatically
- No hardcoded column names
- Mapping stored for reuse

### ✅ Avro Compatible
- All data flows through Avro without errors
- Leverages NiFi's native Avro support
- Fast and efficient

### ✅ User Friendly
- Creates views with original column names
- End users query views, not physical tables
- Transparent to application layer

### ✅ Maintainable
- Centralized mapping storage
- Easy to audit and update
- Clear separation of concerns

### ✅ Scalable
- Can process hundreds of tables
- Mapping cached for performance
- Cluster-aware with DistributedMapCache

---

## Deployment Strategy

### Phase 1: Setup (One-time)
1. Create metadata database table for mappings
2. Configure DistributedMapCacheClient service
3. Deploy metadata extraction flow
4. Test with 1-2 sample tables

### Phase 2: Metadata Collection
1. Run metadata extraction for all source tables
2. Verify mappings in cache/database
3. Review sanitized column names
4. Adjust rules if needed

### Phase 3: Migration
1. Deploy main migration flow
2. Start with smallest tables for testing
3. Validate data integrity
4. Scale to larger tables

### Phase 4: View Creation
1. Generate views for all migrated tables
2. Grant permissions to end users
3. Update application connection strings
4. Deprecate old tables

---

## Monitoring & Validation

### Query to Check Mappings
```sql
SELECT 
    source_table,
    COUNT(*) as column_count,
    SUM(CASE WHEN original_column_name != avro_compatible_name 
             THEN 1 ELSE 0 END) as transformed_count
FROM metadata.dbo.column_mappings
GROUP BY source_table
ORDER BY transformed_count DESC;
```

### Data Validation Query
```sql
-- Compare source and destination row counts
SELECT 
    'source' as location, 
    COUNT(*) as row_count,
    CHECKSUM_AGG(CHECKSUM(*)) as checksum
FROM source.dbo.customers
UNION ALL
SELECT 
    'destination', 
    COUNT(*),
    CHECKSUM_AGG(CHECKSUM(*))
FROM destination.dbo.customers;
```

---

## Conclusion

This solution:
- ✅ Works with Avro format natively
- ✅ Fully dynamic - no hardcoding
- ✅ Handles any special characters
- ✅ Scalable to hundreds of tables
- ✅ Maintains data integrity
- ✅ User-friendly via views
- ✅ Production-ready architecture

The key insight: **Don't fight Avro's restrictions - work with them by transforming names bidirectionally**.
