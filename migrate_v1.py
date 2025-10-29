"""
Data Ingestion Module: Databricks Volumes to SQLite
Migrated from SQL Server source to Databricks Volumes
"""

import sqlite3
from pathlib import Path
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession
import pandas as pd


class DatabricksVolumeIngestion:
    """
    Handles data ingestion from Databricks Volumes to local SQLite database.
    
    Attributes:
        spark: SparkSession for reading data from Databricks
        sqlite_path: Path to local SQLite database
        volume_base_path: Base path to Unity Catalog volume
    """
    
    def __init__(
        self,
        sqlite_db_path: str,
        catalog: str,
        schema: str,
        volume: str,
        spark: Optional[SparkSession] = None
    ):
        """
        Initialize the ingestion handler.
        
        Args:
            sqlite_db_path: Path to SQLite database file
            catalog: Unity Catalog name
            schema: Schema name within catalog
            volume: Volume name within schema
            spark: Existing SparkSession (creates new if None)
        """
        self.sqlite_path = sqlite_db_path
        self.volume_base_path = f"/Volumes/{catalog}/{schema}/{volume}"
        
        # Initialize or use existing Spark session
        if spark is None:
            self.spark = SparkSession.builder \
                .appName("DatabricksVolumeIngestion") \
                .getOrCreate()
        else:
            self.spark = spark
    
    def read_query_file(self, query_file_path: str) -> str:
        """
        Read SQL query from .sql file.
        
        Args:
            query_file_path: Path to .sql file
            
        Returns:
            SQL query string
        """
        with open(query_file_path, 'r') as f:
            query = f.read().strip()
        return query
    
    def convert_sql_server_to_spark(self, query: str) -> str:
        """
        Convert common SQL Server syntax to Spark SQL.
        
        Args:
            query: Original SQL Server query
            
        Returns:
            Spark SQL compatible query
        """
        # Replace square brackets with backticks
        query = query.replace('[', '`').replace(']', '`')
        
        # Replace TOP N with LIMIT N (basic pattern)
        import re
        query = re.sub(
            r'\bSELECT\s+TOP\s+(\d+)\b',
            r'SELECT',
            query,
            flags=re.IGNORECASE
        )
        # Add LIMIT at the end if TOP was found
        top_match = re.search(r'\bTOP\s+(\d+)\b', query, re.IGNORECASE)
        if top_match and 'LIMIT' not in query.upper():
            limit_num = top_match.group(1)
            query = query + f' LIMIT {limit_num}'
        
        # Replace common SQL Server functions
        replacements = {
            'GETDATE()': 'current_timestamp()',
            'ISNULL(': 'COALESCE(',
            'LEN(': 'LENGTH(',
        }
        
        for old, new in replacements.items():
            query = query.replace(old, new)
            query = query.replace(old.lower(), new.lower())
        
        return query
    
    def ingest_from_volume_table(
        self,
        table_path: str,
        sqlite_table_name: str,
        query: Optional[str] = None,
        file_format: str = 'parquet',
        if_exists: str = 'replace',
        chunksize: Optional[int] = None,
        transform_query: bool = True
    ) -> Dict[str, Any]:
        """
        Ingest data from Databricks Volume file to SQLite.
        
        Args:
            table_path: Relative path to table within volume (e.g., 'raw_data/customers')
            sqlite_table_name: Target table name in SQLite
            query: Optional SQL query to filter/transform data. If None, reads all data.
            file_format: File format in volume ('parquet', 'delta', 'csv', 'json')
            if_exists: How to handle existing table ('replace', 'append', 'fail')
            chunksize: If set, write to SQLite in chunks (useful for large datasets)
            transform_query: Whether to apply SQL Server to Spark transformations
            
        Returns:
            Dictionary with ingestion statistics
        """
        # Construct full volume path
        full_path = f"{self.volume_base_path}/{table_path}"
        
        try:
            # Read data from Databricks Volume
            if file_format == 'delta':
                df_spark = self.spark.read.format('delta').load(full_path)
            elif file_format == 'parquet':
                df_spark = self.spark.read.parquet(full_path)
            elif file_format == 'csv':
                df_spark = self.spark.read.option('header', 'true').csv(full_path)
            elif file_format == 'json':
                df_spark = self.spark.read.json(full_path)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
            
            # Apply query if provided
            if query:
                if transform_query:
                    query = self.convert_sql_server_to_spark(query)
                
                # Register as temp view and query
                df_spark.createOrReplaceTempView("temp_table")
                # Replace FROM table_name with FROM temp_table
                import re
                query = re.sub(
                    r'\bFROM\s+\w+',
                    'FROM temp_table',
                    query,
                    flags=re.IGNORECASE,
                    count=1
                )
                df_spark = self.spark.sql(query)
            
            # Convert to Pandas for SQLite write
            df_pandas = df_spark.toPandas()
            
            # Write to SQLite
            with sqlite3.connect(self.sqlite_path) as conn:
                if chunksize:
                    # Write in chunks for large datasets
                    for i in range(0, len(df_pandas), chunksize):
                        chunk = df_pandas.iloc[i:i+chunksize]
                        chunk.to_sql(
                            sqlite_table_name,
                            conn,
                            if_exists='append' if i > 0 else if_exists,
                            index=False
                        )
                else:
                    df_pandas.to_sql(
                        sqlite_table_name,
                        conn,
                        if_exists=if_exists,
                        index=False
                    )
            
            return {
                'status': 'success',
                'rows_ingested': len(df_pandas),
                'columns': len(df_pandas.columns),
                'table_name': sqlite_table_name
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'table_name': sqlite_table_name
            }
    
    def ingest_from_query_file(
        self,
        query_file_path: str,
        table_path: str,
        sqlite_table_name: str,
        file_format: str = 'parquet',
        if_exists: str = 'replace',
        chunksize: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Ingest data using query from .sql file.
        
        Args:
            query_file_path: Path to .sql file containing SELECT query
            table_path: Relative path to table within volume
            sqlite_table_name: Target table name in SQLite
            file_format: File format in volume
            if_exists: How to handle existing table
            chunksize: If set, write to SQLite in chunks
            
        Returns:
            Dictionary with ingestion statistics
        """
        query = self.read_query_file(query_file_path)
        return self.ingest_from_volume_table(
            table_path=table_path,
            sqlite_table_name=sqlite_table_name,
            query=query,
            file_format=file_format,
            if_exists=if_exists,
            chunksize=chunksize
        )
    
    def batch_ingest(
        self,
        ingestion_config: list[Dict[str, Any]]
    ) -> list[Dict[str, Any]]:
        """
        Ingest multiple tables in batch.
        
        Args:
            ingestion_config: List of configuration dicts with keys:
                - table_path: Path to table in volume
                - sqlite_table_name: Target SQLite table
                - query_file: Optional path to .sql file
                - file_format: Optional file format (default: 'parquet')
                - if_exists: Optional behavior (default: 'replace')
                
        Returns:
            List of ingestion result dictionaries
        """
        results = []
        
        for config in ingestion_config:
            if 'query_file' in config:
                result = self.ingest_from_query_file(
                    query_file_path=config['query_file'],
                    table_path=config['table_path'],
                    sqlite_table_name=config['sqlite_table_name'],
                    file_format=config.get('file_format', 'parquet'),
                    if_exists=config.get('if_exists', 'replace'),
                    chunksize=config.get('chunksize')
                )
            else:
                result = self.ingest_from_volume_table(
                    table_path=config['table_path'],
                    sqlite_table_name=config['sqlite_table_name'],
                    query=config.get('query'),
                    file_format=config.get('file_format', 'parquet'),
                    if_exists=config.get('if_exists', 'replace'),
                    chunksize=config.get('chunksize')
                )
            
            results.append(result)
        
        return results


# Example usage
if __name__ == "__main__":
    
    # Initialize ingestion handler
    ingestion = DatabricksVolumeIngestion(
        sqlite_db_path='./data/local_database.db',
        catalog='main',
        schema='data_ingestion',
        volume='raw_files'
    )
    
    # Example 1: Ingest with query file
    result1 = ingestion.ingest_from_query_file(
        query_file_path='./queries/customer_query.sql',
        table_path='customers',
        sqlite_table_name='customers',
        file_format='parquet'
    )
    print(f"Ingestion 1: {result1}")
    
    # Example 2: Ingest entire table without query
    result2 = ingestion.ingest_from_volume_table(
        table_path='orders',
        sqlite_table_name='orders',
        file_format='delta',
        if_exists='replace'
    )
    print(f"Ingestion 2: {result2}")
    
    # Example 3: Batch ingestion
    batch_config = [
        {
            'table_path': 'products',
            'sqlite_table_name': 'products',
            'query_file': './queries/products.sql',
            'file_format': 'parquet'
        },
        {
            'table_path': 'transactions',
            'sqlite_table_name': 'transactions',
            'file_format': 'delta',
            'chunksize': 10000  # Write in chunks for large dataset
        }
    ]
    
    results = ingestion.batch_ingest(batch_config)
    for result in results:
        print(f"Batch result: {result}")
