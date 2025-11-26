"""
Monthly Batch Processing Workflow Examples
===========================================

This module demonstrates typical monthly workflow patterns for the batch processing system.
"""

from batch_orchestrator import BedrockBatchOrchestrator
from batch_log_manager import BatchLogManager
from batch_utilities import BatchAnalytics
from typing import Optional

# ============================================================================
# MONTHLY WORKFLOW: Complete Example
# ============================================================================

class MonthlyBatchWorkflow:
    """
    Orchestrates a complete monthly batch processing workflow.
    """
    
    def __init__(self, catalog: str = "main", schema: str = "default"):
        self.orchestrator = BedrockBatchOrchestrator(
            aws_region="us-east-1",
            catalog=catalog,
            schema=schema
        )
        self.log_manager = BatchLogManager(catalog=catalog, schema=schema)
        self.analytics = BatchAnalytics(catalog=catalog, schema=schema)
    
    def run_monthly_batch_submission(self, year: int, month: int):
        """
        STEP 1: Run at the beginning of the month to submit all batches.
        
        Example schedule: 1st day of each month at 1:00 AM
        
        Args:
            year: Year (e.g., 2024)
            month: Month (1-12)
        """
        yearmonth = BatchLogManager.parse_yearmonth(year, month)
        
        print("\n" + "=" * 80)
        print(f"MONTHLY BATCH SUBMISSION - {yearmonth}")
        print("=" * 80 + "\n")
        
        # Define your batch files
        jsonl_files = self._get_monthly_batch_files(year, month)
        
        # Submit all batches
        batch_ids = self.orchestrator.submit_all_batches(
            jsonl_files=jsonl_files,
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            input_s3_base_uri=f"s3://my-bucket/bedrock-inputs/{yearmonth}/",
            output_s3_base_uri=f"s3://my-bucket/bedrock-outputs/{yearmonth}/",
            role_arn="arn:aws:iam::123456789012:role/BedrockBatchRole",
            prompts_per_batch=200,
            yearmonth=yearmonth
        )
        
        print(f"\n✓ Successfully submitted {len(batch_ids)} batches for {yearmonth}")
        
        return batch_ids
    
    def run_hourly_status_check(self, yearmonth: Optional[str] = None):
        """
        STEP 2: Run hourly to check and update batch statuses.
        
        Example schedule: Every hour
        
        Args:
            yearmonth: Optional specific yearmonth. If None, checks current month.
        """
        if yearmonth is None:
            yearmonth = BatchLogManager.get_current_yearmonth()
        
        stats = self.orchestrator.update_batch_statuses(yearmonth=yearmonth)
        
        # Check if we need to send alerts
        if stats['errors'] > 10:
            self._send_alert(
                f"High error rate detected for {yearmonth}: {stats['errors']} errors",
                severity="WARNING"
            )
        
        return stats
    
    def run_end_of_month_report(self, yearmonth: str):
        """
        STEP 3: Run at the end of the month to generate final report.
        
        Example schedule: Last day of each month at 11:00 PM
        
        Args:
            yearmonth: The yearmonth to report on (format: YYYYMM)
        """
        print("\n" + "=" * 80)
        print(f"END OF MONTH REPORT - {yearmonth}")
        print("=" * 80 + "\n")
        
        # Generate comprehensive report
        report = self.analytics.generate_status_report(yearmonth=yearmonth)
        print(report)
        
        # Export results
        self.analytics.export_results_summary(
            f"/dbfs/mnt/reports/monthly_report_{yearmonth}.csv",
            yearmonth=yearmonth
        )
        
        # Check for any outstanding issues
        stats = self.log_manager.get_statistics(yearmonth=yearmonth)
        
        pending = stats.get('PENDING', {}).get('count', 0)
        processing = stats.get('PROCESSING', {}).get('count', 0)
        
        if pending > 0 or processing > 0:
            warning = f"WARNING: {yearmonth} has {pending + processing} batches still in progress!"
            print("\n" + "!" * 80)
            print(warning)
            print("!" * 80 + "\n")
            self._send_alert(warning, severity="WARNING")
        
        return report
    
    def cleanup_previous_months(self, months_to_keep: int = 3):
        """
        STEP 4: Run monthly to clean up old data.
        
        Example schedule: 1st day of each month at 2:00 AM
        
        Args:
            months_to_keep: Number of months of data to retain
        """
        print("\n" + "=" * 80)
        print(f"CLEANUP: Retaining last {months_to_keep} months")
        print("=" * 80 + "\n")
        
        # Calculate days to keep
        days_to_keep = months_to_keep * 30
        
        deleted_count = self.log_manager.cleanup_old_records(
            days_to_keep=days_to_keep
        )
        
        print(f"✓ Cleaned up {deleted_count} old batch records")
        
        return deleted_count
    
    def _get_monthly_batch_files(self, year: int, month: int) -> list:
        """
        Get list of JSONL files for the month.
        Customize this based on your file organization.
        """
        # Example: Files are organized by year/month
        base_path = f"/dbfs/mnt/data/batches/{year}/{month:02d}/"
        
        # Generate list of batch files (1000 files with 200 prompts each)
        jsonl_files = [
            f"{base_path}batch_{i:04d}.jsonl"
            for i in range(1, 1001)
        ]
        
        return jsonl_files
    
    def _send_alert(self, message: str, severity: str = "INFO"):
        """
        Send alert notification.
        Customize with your notification system (Slack, email, PagerDuty, etc.)
        """
        print(f"\n[{severity}] ALERT: {message}\n")
        # Add your notification logic here
        # Example: send_slack_message(message)
        # Example: send_email(message)


# ============================================================================
# DATABRICKS JOB DEFINITIONS
# ============================================================================

# Initialize workflow
workflow = MonthlyBatchWorkflow(catalog="main", schema="bedrock_batches")

# ----------------------------------------------------------------------------
# Job 1: Monthly Batch Submission
# Schedule: 1st of every month at 1:00 AM
# ----------------------------------------------------------------------------
def job_monthly_submission():
    """
    Main job for monthly batch submission.
    This should be scheduled to run on the 1st day of each month.
    """
    from datetime import datetime
    
    # Get current year and month
    now = datetime.now()
    year = now.year
    month = now.month
    
    batch_ids = workflow.run_monthly_batch_submission(year, month)
    
    return {
        'status': 'success',
        'yearmonth': f"{year}{month:02d}",
        'batches_submitted': len(batch_ids)
    }


# ----------------------------------------------------------------------------
# Job 2: Hourly Status Check
# Schedule: Every hour
# ----------------------------------------------------------------------------
def job_hourly_status_check():
    """
    Hourly job to check and update batch statuses.
    This should be scheduled to run every hour.
    """
    stats = workflow.run_hourly_status_check()
    
    return {
        'status': 'success',
        'batches_checked': stats['checked'],
        'batches_updated': stats['updated'],
        'errors': stats['errors']
    }


# ----------------------------------------------------------------------------
# Job 3: End of Month Report
# Schedule: Last day of every month at 11:00 PM
# ----------------------------------------------------------------------------
def job_end_of_month_report():
    """
    Generate end-of-month report.
    This should be scheduled to run on the last day of each month.
    """
    from datetime import datetime
    
    # Get current yearmonth
    now = datetime.now()
    yearmonth = f"{now.year}{now.month:02d}"
    
    report = workflow.run_end_of_month_report(yearmonth)
    
    return {
        'status': 'success',
        'yearmonth': yearmonth,
        'report_generated': True
    }


# ----------------------------------------------------------------------------
# Job 4: Monthly Cleanup
# Schedule: 1st of every month at 2:00 AM
# ----------------------------------------------------------------------------
def job_monthly_cleanup():
    """
    Clean up old batch records.
    This should be scheduled to run on the 1st day of each month.
    """
    deleted_count = workflow.cleanup_previous_months(months_to_keep=3)
    
    return {
        'status': 'success',
        'records_deleted': deleted_count
    }


# ============================================================================
# AD-HOC QUERIES AND MAINTENANCE
# ============================================================================

def check_specific_month(year: int, month: int):
    """
    Check status of a specific month's batches.
    
    Usage:
        check_specific_month(2024, 11)
    """
    yearmonth = BatchLogManager.parse_yearmonth(year, month)
    
    log_manager = BatchLogManager()
    stats = log_manager.get_statistics(yearmonth=yearmonth)
    
    print(f"\nStatistics for {yearmonth}:")
    print("-" * 40)
    for status, metrics in stats.items():
        print(f"\n{status}:")
        print(f"  Batches: {metrics['count']}")
        print(f"  Total prompts: {metrics['total_prompts']}")
        print(f"  Completed: {metrics['completed_prompts']}")
        print(f"  Failed: {metrics['failed_prompts']}")
    
    return stats


def resubmit_failed_batches(yearmonth: str):
    """
    Resubmit all failed batches for a specific month.
    
    Usage:
        resubmit_failed_batches('202411')
    """
    log_manager = BatchLogManager()
    
    # Get failed batches
    failed_df = log_manager.spark.sql(f"""
        SELECT *
        FROM {log_manager.full_table_name}
        WHERE batch_status = 'FAILED'
          AND yearmonth = '{yearmonth}'
    """)
    
    failed_batches = [row.asDict() for row in failed_df.collect()]
    
    print(f"Found {len(failed_batches)} failed batches to resubmit")
    
    # Here you would implement resubmission logic
    # This is a placeholder - adjust based on your requirements
    for batch in failed_batches:
        print(f"Would resubmit: {batch['batch_id']} - {batch['jsonl_file_path']}")
    
    return failed_batches


def get_monthly_summary():
    """
    Get a summary of all months in the system.
    
    Usage:
        summary = get_monthly_summary()
    """
    log_manager = BatchLogManager()
    
    summary_df = log_manager.spark.sql(f"""
        SELECT 
            yearmonth,
            COUNT(*) as total_batches,
            SUM(CASE WHEN batch_status = 'COMPLETED' THEN 1 ELSE 0 END) as completed,
            SUM(CASE WHEN batch_status = 'FAILED' THEN 1 ELSE 0 END) as failed,
            SUM(CASE WHEN batch_status IN ('PENDING', 'PROCESSING') THEN 1 ELSE 0 END) as in_progress,
            SUM(total_prompts) as total_prompts,
            SUM(completed_prompts) as completed_prompts,
            MIN(created_at) as first_batch_date,
            MAX(updated_at) as last_update_date
        FROM {log_manager.full_table_name}
        GROUP BY yearmonth
        ORDER BY yearmonth DESC
    """)
    
    summary_df.show(truncate=False)
    
    return [row.asDict() for row in summary_df.collect()]


def force_complete_stuck_batches(yearmonth: str, hours_threshold: int = 48):
    """
    Manually mark very old stuck batches as failed.
    Use with caution!
    
    Usage:
        force_complete_stuck_batches('202411', hours_threshold=48)
    """
    log_manager = BatchLogManager()
    
    count = log_manager.mark_stuck_batches_as_failed(
        hours_threshold=hours_threshold,
        yearmonth=yearmonth
    )
    
    print(f"Marked {count} stuck batches as FAILED")
    
    return count


# ============================================================================
# MONITORING DASHBOARD QUERIES
# ============================================================================

def get_current_month_dashboard():
    """
    Get real-time dashboard data for current month.
    Suitable for refreshing dashboards or monitoring screens.
    """
    log_manager = BatchLogManager()
    yearmonth = BatchLogManager.get_current_yearmonth()
    
    dashboard_data = {
        'yearmonth': yearmonth,
        'timestamp': datetime.now().isoformat()
    }
    
    # Get statistics
    stats = log_manager.get_statistics(yearmonth=yearmonth)
    dashboard_data['stats'] = stats
    
    # Get stuck batches
    stuck = log_manager.get_stuck_batches(hours_threshold=24, yearmonth=yearmonth)
    dashboard_data['stuck_count'] = len(stuck)
    
    # Calculate progress
    total = sum(s['count'] for s in stats.values())
    completed = stats.get('COMPLETED', {}).get('count', 0)
    dashboard_data['progress_pct'] = (completed / total * 100) if total > 0 else 0
    
    return dashboard_data


# ============================================================================
# EXAMPLE USAGE IN NOTEBOOK
# ============================================================================

if __name__ == "__main__":
    # Example: Run monthly submission for November 2024
    # workflow.run_monthly_batch_submission(2024, 11)
    
    # Example: Check status of current month
    # workflow.run_hourly_status_check()
    
    # Example: Generate report for specific month
    # workflow.run_end_of_month_report('202411')
    
    # Example: Check specific month
    # check_specific_month(2024, 11)
    
    # Example: Get summary of all months
    # get_monthly_summary()
    
    pass




# November 2024 batch run
workflow = MonthlyBatchWorkflow()

# 1. Submit batches (1st of month)
workflow.run_monthly_batch_submission(2024, 11)

# 2. Check status hourly
workflow.run_hourly_status_check(yearmonth='202411')

# 3. Generate report (end of month)
workflow.run_end_of_month_report('202411')

# 4. Analyze specific month
check_specific_month(2024, 11)
