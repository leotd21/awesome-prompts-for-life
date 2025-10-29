from pyspark.sql import SparkSession
import pandas as pd
import sqlite3
from pathlib import Path
from typing import Optional, Dict, Any

class DatabricksToSQLiteIngestion:
    def __init__(
        self,
        databricks_host: str,
        databricks_token: str,
        sqlite_db_path: str,
        catalog: str = "main",
        schema: str = "default"
    ):
        """
        Initialize connection parameters.
        
        Args:
            databricks_host: Your workspace URL (e.g., 'https://adb-123456789.azuredatabricks.net')
            databricks_token: Personal access token or service principal token
            sqlite_db_path: Local path to SQLite database file
            catalog: Unity Catalog name (default: 'main')
            schema: Schema name within catalog (default: 'default')
        """
        self.spark = self._create_spark_session(databricks_host, databricks_token)
        self.sqlite_db_path = sqlite_db_path
        self.catalog = catalog
        self.schema = schema
        
    def _create_spark_session(self, host: str, token: str) -> SparkSession:
        """Create Spark session with Databricks connection."""
        return (SparkSession.builder
                .appName("DatabricksToSQLite")
                .config("spark.databricks.service.address", host)
                .config("spark.databricks.service.token", token)
                # For remote connection - if running locally
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .getOrCreate())
    
    def read_from_delta_table(self, table_name: str, query_file: Optional[str] = None) -> pd.DataFrame:
        """
        Read from Delta table using existing SQL query files.
        
        Args:
            table_name: Base table name (queries will be modified to include catalog.schema prefix)
            query_file: Path to .sql file with SELECT query (optional)
            
        Returns:
            Pandas DataFrame with query results
        """
        if query_file:
            query = self._load_and_modify_query(query_file, table_name)
        else:
            # Simple full table read
            query = f"SELECT * FROM {self.catalog}.{self.schema}.{table_name}"
        
        # Execute query with Spark
        spark_df = self.spark.sql(query)
        
        # Convert to Pandas (happens in-memory, suitable for moderate datasets)
        return spark_df.toPandas()
    
    def read_from_volume_file(
        self, 
        volume_name: str, 
        file_path: str, 
        file_format: str = "parquet",
        options: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """
        Read from files in Databricks Volumes.
        
        Args:
            volume_name: Volume name in Unity Catalog
            file_path: Path within volume (e.g., 'sales/2024/data.parquet')
            file_format: File format (parquet, csv, json, delta)
            options: Additional read options (e.g., {'header': 'true'} for CSV)
            
        Returns:
            Pandas DataFrame
        """
        full_path = f"/Volumes/{self.catalog}/{self.schema}/{volume_name}/{file_path}"
        
        # Read based on format
        reader = self.spark.read.format(file_format)
        
        if options:
            for key, value in options.items():
                reader = reader.option(key, value)
        
        spark_df = reader.load(full_path)
        return spark_df.toPandas()
    
    def _load_and_modify_query(self, query_file: str, table_name: str) -> str:
        """
        Load SQL query from file and modify for Unity Catalog.
        
        Handles these patterns:
        - FROM table_name -> FROM catalog.schema.table_name
        - FROM [table_name] -> FROM catalog.schema.table_name
        - Preserves aliases and JOINs
        """
        with open(query_file, 'r') as f:
            query = f.read()
        
        import re
        
        # Pattern 1: Simple table name
        query = re.sub(
            rf'\bFROM\s+{table_name}\b',
            f'FROM {self.catalog}.{self.schema}.{table_name}',
            query,
            flags=re.IGNORECASE
        )
        
        # Pattern 2: Bracketed table name [table_name]
        query = re.sub(
            rf'\bFROM\s+\[{table_name}\]',
            f'FROM {self.catalog}.{self.schema}.{table_name}',
            query,
            flags=re.IGNORECASE
        )
        
        # Pattern 3: JOIN clauses
        query = re.sub(
            rf'\bJOIN\s+{table_name}\b',
            f'JOIN {self.catalog}.{self.schema}.{table_name}',
            query,
            flags=re.IGNORECASE
        )
        
        return query
    
    def write_to_sqlite(
        self, 
        df: pd.DataFrame, 
        table_name: str, 
        if_exists: str = 'replace',
        chunk_size: int = 10000
    ):
        """
        Write Pandas DataFrame to SQLite database.
        
        Args:
            df: Pandas DataFrame to write
            table_name: Target SQLite table name
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
            chunk_size: Write in chunks to avoid memory issues (rows per chunk)
        """
        conn = sqlite3.connect(self.sqlite_db_path)
        
        try:
            # Write with chunking for large datasets
            df.to_sql(
                name=table_name,
                con=conn,
                if_exists=if_exists,
                index=False,
                chunksize=chunk_size,
                method='multi'  # Faster multi-row INSERT
            )
            
            # Create index on common columns (optimize as needed)
            if if_exists == 'replace':
                # Example: Create index on id column if it exists
                if 'id' in df.columns:
                    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_id ON {table_name}(id)")
            
            conn.commit()
            
        finally:
            conn.close()
    
    def ingest_pipeline(
        self,
        source_type: str,  # 'delta_table' or 'volume_file'
        source_config: Dict[str, Any],
        target_table: str,
        if_exists: str = 'replace'
    ):
        """
        Complete ingestion pipeline from Databricks to SQLite.
        
        Args:
            source_type: Type of source ('delta_table' or 'volume_file')
            source_config: Configuration dict specific to source type
            target_table: Target SQLite table name
            if_exists: Behavior if table exists
            
        Example for delta_table:
            source_config = {
                'table_name': 'sales_data',
                'query_file': 'queries/sales.sql'  # optional
            }
            
        Example for volume_file:
            source_config = {
                'volume_name': 'raw_data',
                'file_path': 'sales/2024/jan.parquet',
                'file_format': 'parquet',
                'options': {'mergeSchema': 'true'}  # optional
            }
        """
        print(f"Starting ingestion: {source_type} -> {target_table}")
        
        # Read from source
        if source_type == 'delta_table':
            df = self.read_from_delta_table(**source_config)
        elif source_type == 'volume_file':
            df = self.read_from_volume_file(**source_config)
        else:
            raise ValueError(f"Unknown source_type: {source_type}")
        
        print(f"Read {len(df)} rows from source")
        
        # Write to SQLite
        self.write_to_sqlite(df, target_table, if_exists)
        
        print(f"Successfully wrote to SQLite table: {target_table}")
        
        return df  # Return for validation/inspection

# USAGE EXAMPLE
if __name__ == "__main__":
    # Initialize
    ingestion = DatabricksToSQLiteIngestion(
        databricks_host="https://adb-1234567890123456.7.azuredatabricks.net",
        databricks_token="dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
        sqlite_db_path="local_data.db",
        catalog="prod_catalog",
        schema="bronze_schema"
    )
    
    # Option 1: Read from Delta table with existing SQL query
    ingestion.ingest_pipeline(
        source_type='delta_table',
        source_config={
            'table_name': 'sales_data',
            'query_file': 'queries/sales_query.sql'
        },
        target_table='sales_local'
    )
    
    # Option 2: Read from files in Volumes
    ingestion.ingest_pipeline(
        source_type='volume_file',
        source_config={
            'volume_name': 'raw_sales',
            'file_path': 'january/sales.parquet',
            'file_format': 'parquet'
        },
        target_table='sales_january_local'
    )
