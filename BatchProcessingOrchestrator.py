import boto3
import json
import time
from pathlib import Path
from typing import List, Dict, Optional
from batch_log_manager import BatchLogManager

class BedrockBatchOrchestrator:
    """
    Orchestrates AWS Bedrock batch processing with Databricks tracking.
    """
    
    def __init__(self, 
                 aws_region: str = "us-east-1",
                 catalog: str = "main",
                 schema: str = "default"):
        """
        Initialize the orchestrator.
        
        Args:
            aws_region: AWS region for Bedrock
            catalog: Databricks catalog
            schema: Databricks schema
        """
        self.bedrock = boto3.client('bedrock', region_name=aws_region)
        self.log_manager = BatchLogManager(catalog=catalog, schema=schema)
        self.job_context = None
        
        # Try to get Databricks job context
        try:
            from dbruntime.databricks_repl_context import get_context
            self.job_context = get_context()
        except:
            pass
    
    def create_batch_from_jsonl(self, 
                                jsonl_file_path: str,
                                model_id: str,
                                input_s3_uri: str,
                                output_s3_uri: str,
                                role_arn: str) -> str:
        """
        Create a batch job in AWS Bedrock.
        
        Args:
            jsonl_file_path: Path to JSONL file (for logging purposes)
            model_id: Bedrock model ID
            input_s3_uri: S3 URI of input JSONL file
            output_s3_uri: S3 URI for output
            role_arn: IAM role ARN for Bedrock
            
        Returns:
            batch_id: The created batch job ID
        """
        response = self.bedrock.create_model_invocation_job(
            modelId=model_id,
            jobName=f"batch_{Path(jsonl_file_path).stem}_{int(time.time())}",
            roleArn=role_arn,
            inputDataConfig={
                's3InputDataConfig': {
                    's3Uri': input_s3_uri
                }
            },
            outputDataConfig={
                's3OutputDataConfig': {
                    's3Uri': output_s3_uri
                }
            }
        )
        
        return response['jobArn'].split('/')[-1]  # Extract batch_id from ARN
    
    def get_batch_status(self, batch_id: str) -> Dict:
        """
        Get the status of a batch job from Bedrock.
        
        Args:
            batch_id: Batch job ID
            
        Returns:
            Dictionary with status information
        """
        response = self.bedrock.get_model_invocation_job(
            jobIdentifier=batch_id
        )
        
        return {
            'status': response['status'],
            'input_s3_uri': response['inputDataConfig']['s3InputDataConfig']['s3Uri'],
            'output_s3_uri': response['outputDataConfig']['s3OutputDataConfig'].get('s3Uri'),
            'submitted_at': response.get('submitTime'),
            'completed_at': response.get('endTime'),
            'failure_message': response.get('message'),
            # Parse metrics if available
            'total_prompts': response.get('inputRecordCount', 0),
            'completed_prompts': response.get('outputRecordCount', 0),
            'failed_prompts': response.get('failureCount', 0)
        }
    
    def submit_all_batches(self, 
                          jsonl_files: List[str],
                          model_id: str,
                          input_s3_base_uri: str,
                          output_s3_base_uri: str,
                          role_arn: str,
                          prompts_per_batch: int = 200,
                          yearmonth: Optional[str] = None) -> List[str]:
        """
        Submit all JSONL files as batch jobs.
        
        Args:
            jsonl_files: List of local JSONL file paths
            model_id: Bedrock model ID
            input_s3_base_uri: Base S3 URI for inputs (files should already be uploaded)
            output_s3_base_uri: Base S3 URI for outputs
            role_arn: IAM role ARN
            prompts_per_batch: Expected prompts per batch
            yearmonth: Year-month partition (format: YYYYMM). If None, uses current month.
            
        Returns:
            List of created batch IDs
        """
        batch_ids = []
        job_name = self.job_context.jobName if self.job_context else None
        job_run_id = self.job_context.jobRunId if self.job_context else None
        
        # Use current yearmonth if not provided
        if yearmonth is None:
            yearmonth = BatchLogManager.get_current_yearmonth()
        
        print(f"Processing batches for yearmonth: {yearmonth}")
        
        for jsonl_file in jsonl_files:
            try:
                file_name = Path(jsonl_file).name
                input_s3_uri = f"{input_s3_base_uri.rstrip('/')}/{file_name}"
                output_s3_uri = f"{output_s3_base_uri.rstrip('/')}/{file_name.replace('.jsonl', '')}"
                
                print(f"Submitting batch for: {file_name}")
                
                # Create batch in Bedrock
                batch_id = self.create_batch_from_jsonl(
                    jsonl_file_path=jsonl_file,
                    model_id=model_id,
                    input_s3_uri=input_s3_uri,
                    output_s3_uri=output_s3_uri,
                    role_arn=role_arn
                )
                
                # Log to tracking table
                self.log_manager.insert_batch(
                    batch_id=batch_id,
                    jsonl_file_path=jsonl_file,
                    total_prompts=prompts_per_batch,
                    model_id=model_id,
                    input_s3_uri=input_s3_uri,
                    output_s3_uri=output_s3_uri,
                    job_name=job_name,
                    job_run_id=job_run_id,
                    yearmonth=yearmonth
                )
                
                batch_ids.append(batch_id)
                print(f"✓ Created batch: {batch_id}")
                
                # Small delay to avoid rate limiting
                time.sleep(0.5)
                
            except Exception as e:
                print(f"✗ Error submitting {jsonl_file}: {str(e)}")
                continue
        
        print(f"\n✓ Successfully submitted {len(batch_ids)}/{len(jsonl_files)} batches")
        return batch_ids
    
    def update_batch_statuses(self, batch_limit: Optional[int] = None,
                             yearmonth: Optional[str] = None) -> Dict:
        """
        Check and update statuses for all non-terminal batches.
        This is the main function for the hourly scheduled job.
        
        Args:
            batch_limit: Optional limit on number of batches to check
            yearmonth: Optional filter by yearmonth partition (format: YYYYMM). 
                      If None, checks current month.
            
        Returns:
            Summary statistics
        """
        # Use current yearmonth if not provided
        if yearmonth is None:
            yearmonth = BatchLogManager.get_current_yearmonth()
        
        print("=" * 80)
        print(f"Starting batch status update: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Target yearmonth partition: {yearmonth}")
        print("=" * 80)
        
        # Get non-terminal batches
        batches = self.log_manager.get_non_terminal_batches(
            yearmonth=yearmonth,
            limit=batch_limit
        )
        print(f"\nFound {len(batches)} non-terminal batches to check")
        
        if not batches:
            print("No batches to update. Exiting.")
            return {'checked': 0, 'updated': 0, 'errors': 0}
        
        stats = {'checked': 0, 'updated': 0, 'errors': 0, 'by_status': {}}
        
        for batch in batches:
            batch_id = batch['batch_id']
            old_status = batch['batch_status']
            
            try:
                print(f"\nChecking batch: {batch_id} (current: {old_status})")
                
                # Get current status from Bedrock
                bedrock_status = self.get_batch_status(batch_id)
                new_status = bedrock_status['status']
                
                stats['checked'] += 1
                
                # Map Bedrock statuses to our statuses
                status_mapping = {
                    'InProgress': BatchLogManager.STATUS_PROCESSING,
                    'Completed': BatchLogManager.STATUS_COMPLETED,
                    'Failed': BatchLogManager.STATUS_FAILED,
                    'Stopping': BatchLogManager.STATUS_PROCESSING,
                    'Stopped': BatchLogManager.STATUS_CANCELLED,
                    'Submitted': BatchLogManager.STATUS_PENDING,
                    'Validating': BatchLogManager.STATUS_PENDING
                }
                
                mapped_status = status_mapping.get(new_status, new_status)
                
                # Update if status changed
                if mapped_status != old_status:
                    self.log_manager.update_batch_status(
                        batch_id=batch_id,
                        new_status=mapped_status,
                        completed_prompts=bedrock_status.get('completed_prompts'),
                        failed_prompts=bedrock_status.get('failed_prompts'),
                        output_s3_uri=bedrock_status.get('output_s3_uri'),
                        error_message=bedrock_status.get('failure_message')
                    )
                    
                    print(f"  Status changed: {old_status} → {mapped_status}")
                    stats['updated'] += 1
                    stats['by_status'][mapped_status] = stats['by_status'].get(mapped_status, 0) + 1
                else:
                    print(f"  Status unchanged: {mapped_status}")
                
                # Small delay to avoid rate limiting
                time.sleep(0.2)
                
            except Exception as e:
                print(f"  ✗ Error checking batch {batch_id}: {str(e)}")
                stats['errors'] += 1
                
                # Increment retry count
                if self.log_manager.should_retry(batch_id):
                    retry_count = self.log_manager.increment_retry_count(batch_id)
                    print(f"  Retry count incremented to: {retry_count}")
                else:
                    print(f"  Max retries reached, marking as FAILED")
                    self.log_manager.update_batch_status(
                        batch_id=batch_id,
                        new_status=BatchLogManager.STATUS_FAILED,
                        error_message=f"Max retries exceeded. Last error: {str(e)}"
                    )
        
        # Check for stuck batches
        print("\n" + "-" * 80)
        print("Checking for stuck batches...")
        stuck_count = self.log_manager.mark_stuck_batches_as_failed(
            hours_threshold=24,
            yearmonth=yearmonth
        )
        if stuck_count > 0:
            print(f"Marked {stuck_count} stuck batches as FAILED")
        else:
            print("No stuck batches found")
        
        # Print summary
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"Yearmonth: {yearmonth}")
        print(f"Batches checked: {stats['checked']}")
        print(f"Batches updated: {stats['updated']}")
        print(f"Errors encountered: {stats['errors']}")
        print(f"Stuck batches marked as failed: {stuck_count}")
        
        if stats['by_status']:
            print("\nStatus changes:")
            for status, count in stats['by_status'].items():
                print(f"  {status}: {count}")
        
        # Print overall statistics for this yearmonth
        print("\n" + "-" * 80)
        print(f"OVERALL STATISTICS FOR {yearmonth}")
        print("-" * 80)
        all_stats = self.log_manager.get_statistics(yearmonth=yearmonth)
        for status, metrics in all_stats.items():
            print(f"\n{status}:")
            print(f"  Count: {metrics['count']}")
            print(f"  Total prompts: {metrics['total_prompts']}")
            print(f"  Completed prompts: {metrics['completed_prompts']}")
            print(f"  Failed prompts: {metrics['failed_prompts']}")
        
        print("\n" + "=" * 80)
        
        return stats


# ============================================================================
# EXAMPLE USAGE IN DATABRICKS NOTEBOOKS
# ============================================================================

# Initialize orchestrator
orchestrator = BedrockBatchOrchestrator(
    aws_region="us-east-1",
    catalog="main",
    schema="bedrock_batches"
)

# ============================================================================
# JOB 1: Submit all batches (run once per month)
# ============================================================================
def submit_batches_job(year: int = 2024, month: int = 11):
    """
    Initial job to submit all JSONL files as batches.
    Run this at the beginning of each month.
    
    Args:
        year: Year for the batch run
        month: Month for the batch run (1-12)
    """
    # Generate yearmonth
    yearmonth = BatchLogManager.parse_yearmonth(year, month)
    print(f"Submitting batches for yearmonth: {yearmonth}")
    
    jsonl_files = [
        "/dbfs/mnt/data/batches/batch_001.jsonl",
        "/dbfs/mnt/data/batches/batch_002.jsonl",
        # ... add all 1000 files
    ]
    
    batch_ids = orchestrator.submit_all_batches(
        jsonl_files=jsonl_files,
        model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
        input_s3_base_uri="s3://my-bucket/bedrock-inputs/",
        output_s3_base_uri="s3://my-bucket/bedrock-outputs/",
        role_arn="arn:aws:iam::123456789012:role/BedrockBatchRole",
        prompts_per_batch=200,
        yearmonth=yearmonth
    )
    
    print(f"Submitted {len(batch_ids)} batches for {yearmonth}")

# ============================================================================
# JOB 2: Update batch statuses (run hourly)
# ============================================================================
def update_statuses_job(yearmonth: Optional[str] = None):
    """
    Hourly scheduled job to check and update batch statuses.
    
    Args:
        yearmonth: Optional specific yearmonth to check (format: YYYYMM).
                  If None, uses current month.
    """
    stats = orchestrator.update_batch_statuses(yearmonth=yearmonth)
    
    # Optional: Send notification if there are errors
    if stats['errors'] > 10:
        print(f"WARNING: High error rate detected: {stats['errors']} errors")
        # Add notification logic here (email, Slack, etc.)

# ============================================================================
# JOB 3: Cleanup old records (run monthly)
# ============================================================================
def cleanup_job(yearmonth: Optional[str] = None):
    """
    Monthly job to clean up old completed records.
    
    Args:
        yearmonth: Optional specific yearmonth partition to clean.
                  If None, cleans across all partitions.
    """
    deleted_count = orchestrator.log_manager.cleanup_old_records(
        days_to_keep=90,
        yearmonth=yearmonth
    )
    print(f"Cleaned up {deleted_count} old records")
