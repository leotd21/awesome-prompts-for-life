from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from datetime import datetime, timezone
from typing import Optional, List, Dict
import json

class BatchLogManager:
    """
    Manages batch log table operations for AWS Bedrock batch processing in Databricks.
    """
    
    # Status constants
    STATUS_PENDING = 'PENDING'
    STATUS_PROCESSING = 'PROCESSING'
    STATUS_COMPLETED = 'COMPLETED'
    STATUS_FAILED = 'FAILED'
    STATUS_EXPIRED = 'EXPIRED'
    STATUS_CANCELLED = 'CANCELLED'
    
    NON_TERMINAL_STATUSES = [STATUS_PENDING, STATUS_PROCESSING]
    TERMINAL_STATUSES = [STATUS_COMPLETED, STATUS_FAILED, STATUS_EXPIRED, STATUS_CANCELLED]
    
    def __init__(self, catalog: str = "main", schema: str = "default", 
                 table_name: str = "batch_log_table"):
        """
        Initialize BatchLogManager.
        
        Args:
            catalog: Databricks catalog name
            schema: Schema/database name
            table_name: Table name
        """
        self.spark = SparkSession.builder.getOrCreate()
        self.full_table_name = f"{catalog}.{schema}.{table_name}"
        
    def insert_batch(self, batch_id: str, jsonl_file_path: str, 
                     total_prompts: int, model_id: str,
                     input_s3_uri: str, output_s3_uri: Optional[str] = None,
                     job_name: Optional[str] = None, 
                     job_run_id: Optional[str] = None,
                     yearmonth: Optional[str] = None) -> None:
        """
        Insert a new batch record into the log table.
        
        Args:
            batch_id: Unique batch ID from Bedrock
            jsonl_file_path: Path to the source JSONL file
            total_prompts: Number of prompts in this batch
            model_id: AWS Bedrock model ID used
            input_s3_uri: S3 URI of the input data
            output_s3_uri: S3 URI where results will be stored
            job_name: Name of the Databricks job
            job_run_id: Run ID of the Databricks job
            yearmonth: Year-month partition (format: YYYYMM, e.g., '202411'). If None, uses current month.
        """
        now = datetime.now(timezone.utc)
        
        # Generate yearmonth if not provided
        if yearmonth is None:
            yearmonth = now.strftime('%Y%m')
        
        data = [{
            "batch_id": batch_id,
            "jsonl_file_path": jsonl_file_path,
            "batch_status": self.STATUS_PENDING,
            "total_prompts": total_prompts,
            "completed_prompts": 0,
            "failed_prompts": 0,
            "created_at": now,
            "updated_at": now,
            "submitted_at": now,
            "completed_at": None,
            "model_id": model_id,
            "input_s3_uri": input_s3_uri,
            "output_s3_uri": output_s3_uri,
            "error_message": None,
            "retry_count": 0,
            "max_retries": 3,
            "job_name": job_name,
            "job_run_id": job_run_id,
            "yearmonth": yearmonth
        }]
        
        df = self.spark.createDataFrame(data)
        df.write.format("delta").mode("append").saveAsTable(self.full_table_name)
        
        print(f"✓ Inserted batch record: {batch_id}")
    
    def update_batch_status(self, batch_id: str, new_status: str,
                           completed_prompts: Optional[int] = None,
                           failed_prompts: Optional[int] = None,
                           output_s3_uri: Optional[str] = None,
                           error_message: Optional[str] = None) -> None:
        """
        Update the status of a batch.
        
        Args:
            batch_id: Batch ID to update
            new_status: New status value
            completed_prompts: Number of completed prompts
            failed_prompts: Number of failed prompts
            output_s3_uri: Output S3 URI (if available)
            error_message: Error message if status is FAILED
        """
        # Build update expressions
        update_dict = {
            "batch_status": new_status,
            "updated_at": datetime.now(timezone.utc)
        }
        
        if completed_prompts is not None:
            update_dict["completed_prompts"] = completed_prompts
            
        if failed_prompts is not None:
            update_dict["failed_prompts"] = failed_prompts
            
        if output_s3_uri is not None:
            update_dict["output_s3_uri"] = output_s3_uri
            
        if error_message is not None:
            update_dict["error_message"] = error_message
        
        # Mark completion timestamp for terminal statuses
        if new_status in self.TERMINAL_STATUSES:
            update_dict["completed_at"] = datetime.now(timezone.utc)
        
        # Create update expression
        set_clause = ", ".join([f"{k} = '{v}'" if isinstance(v, str) else f"{k} = {v}" 
                               for k, v in update_dict.items() if v is not None])
        
        # Execute update using Delta Lake merge
        self.spark.sql(f"""
            MERGE INTO {self.full_table_name} AS target
            USING (SELECT '{batch_id}' as batch_id) AS source
            ON target.batch_id = source.batch_id
            WHEN MATCHED THEN UPDATE SET {set_clause}
        """)
        
        print(f"✓ Updated batch {batch_id} to status: {new_status}")
    
    def get_non_terminal_batches(self, limit: Optional[int] = None) -> List[Dict]:
        """
        Retrieve all batches with non-terminal statuses.
        
        Args:
            limit: Optional limit on number of records to return
            
        Returns:
            List of batch records as dictionaries
        """
        status_filter = ", ".join([f"'{s}'" for s in self.NON_TERMINAL_STATUSES])
        
        query = f"""
            SELECT *
            FROM {self.full_table_name}
            WHERE batch_status IN ({status_filter})
            ORDER BY created_at ASC
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        df = self.spark.sql(query)
        return [row.asDict() for row in df.collect()]
    
    def get_batch_by_id(self, batch_id: str) -> Optional[Dict]:
        """
        Retrieve a specific batch by ID.
        
        Args:
            batch_id: Batch ID to retrieve
            
        Returns:
            Batch record as dictionary or None if not found
        """
        df = self.spark.sql(f"""
            SELECT *
            FROM {self.full_table_name}
            WHERE batch_id = '{batch_id}'
        """)
        
        rows = df.collect()
        return rows[0].asDict() if rows else None
    
    def increment_retry_count(self, batch_id: str) -> int:
        """
        Increment retry count for a batch.
        
        Args:
            batch_id: Batch ID
            
        Returns:
            New retry count
        """
        # Get current retry count
        batch = self.get_batch_by_id(batch_id)
        if not batch:
            raise ValueError(f"Batch {batch_id} not found")
        
        new_retry_count = batch['retry_count'] + 1
        
        self.spark.sql(f"""
            UPDATE {self.full_table_name}
            SET retry_count = {new_retry_count},
                updated_at = current_timestamp()
            WHERE batch_id = '{batch_id}'
        """)
        
        return new_retry_count
    
    def should_retry(self, batch_id: str) -> bool:
        """
        Check if a batch should be retried based on retry count.
        
        Args:
            batch_id: Batch ID
            
        Returns:
            True if retry count is below max_retries
        """
        batch = self.get_batch_by_id(batch_id)
        if not batch:
            return False
        
        return batch['retry_count'] < batch['max_retries']
    
    def get_statistics(self) -> Dict:
        """
        Get overall statistics of batch processing.
        
        Returns:
            Dictionary with status counts and metrics
        """
        stats_df = self.spark.sql(f"""
            SELECT 
                batch_status,
                COUNT(*) as count,
                SUM(total_prompts) as total_prompts,
                SUM(completed_prompts) as completed_prompts,
                SUM(failed_prompts) as failed_prompts,
                AVG(retry_count) as avg_retry_count
            FROM {self.full_table_name}
            GROUP BY batch_status
            ORDER BY batch_status
        """)
        
        return {row['batch_status']: row.asDict() 
                for row in stats_df.collect()}
    
    def get_stuck_batches(self, hours_threshold: int = 24) -> List[Dict]:
        """
        Find batches that have been in non-terminal status for too long.
        
        Args:
            hours_threshold: Number of hours to consider a batch as stuck
            
        Returns:
            List of potentially stuck batch records
        """
        status_filter = ", ".join([f"'{s}'" for s in self.NON_TERMINAL_STATUSES])
        
        query = f"""
            SELECT *
            FROM {self.full_table_name}
            WHERE batch_status IN ({status_filter})
              AND updated_at < date_sub(current_timestamp(), {hours_threshold}/24)
            ORDER BY updated_at ASC
        """
        
        df = self.spark.sql(query)
        return [row.asDict() for row in df.collect()]
    
    def mark_stuck_batches_as_failed(self, hours_threshold: int = 24) -> int:
        """
        Mark stuck batches as FAILED.
        
        Args:
            hours_threshold: Number of hours to consider a batch as stuck
            
        Returns:
            Number of batches marked as failed
        """
        stuck_batches = self.get_stuck_batches(hours_threshold)
        
        for batch in stuck_batches:
            self.update_batch_status(
                batch_id=batch['batch_id'],
                new_status=self.STATUS_FAILED,
                error_message=f"Batch stuck in {batch['batch_status']} status for over {hours_threshold} hours"
            )
        
        return len(stuck_batches)
    
    def cleanup_old_records(self, days_to_keep: int = 90) -> int:
        """
        Delete old completed batch records.
        
        Args:
            days_to_keep: Number of days to retain records
            
        Returns:
            Number of records deleted
        """
        # Count records to delete
        count_df = self.spark.sql(f"""
            SELECT COUNT(*) as cnt
            FROM {self.full_table_name}
            WHERE completed_at < date_sub(current_timestamp(), {days_to_keep})
              AND batch_status IN ('COMPLETED', 'FAILED', 'CANCELLED')
        """)
        
        count = count_df.collect()[0]['cnt']
        
        # Delete old records
        self.spark.sql(f"""
            DELETE FROM {self.full_table_name}
            WHERE completed_at < date_sub(current_timestamp(), {days_to_keep})
              AND batch_status IN ('COMPLETED', 'FAILED', 'CANCELLED')
        """)
        
        print(f"✓ Deleted {count} old batch records")
        return count
