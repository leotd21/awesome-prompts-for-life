"""
Utility functions for batch processing monitoring and analysis.
"""

from batch_log_manager import BatchLogManager
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg, datediff, current_timestamp
import matplotlib.pyplot as plt
from typing import Dict, List

class BatchAnalytics:
    """
    Analytics and monitoring utilities for batch processing.
    """
    
    def __init__(self, catalog: str = "main", schema: str = "default"):
        self.log_manager = BatchLogManager(catalog=catalog, schema=schema)
        self.spark = SparkSession.builder.getOrCreate()
    
    def get_completion_rate(self, yearmonth: Optional[str] = None) -> float:
        """
        Calculate overall completion rate.
        
        Args:
            yearmonth: Optional filter by yearmonth (format: YYYYMM)
        """
        stats = self.log_manager.get_statistics(yearmonth=yearmonth)
        
        total = sum(s['count'] for s in stats.values())
        completed = stats.get('COMPLETED', {}).get('count', 0)
        
        return (completed / total * 100) if total > 0 else 0
    
    def get_failure_rate(self, yearmonth: Optional[str] = None) -> float:
        """
        Calculate overall failure rate.
        
        Args:
            yearmonth: Optional filter by yearmonth (format: YYYYMM)
        """
        stats = self.log_manager.get_statistics(yearmonth=yearmonth)
        
        total = sum(s['count'] for s in stats.values())
        failed = stats.get('FAILED', {}).get('count', 0)
        
        return (failed / total * 100) if total > 0 else 0
    
    def get_average_processing_time(self, yearmonth: Optional[str] = None) -> float:
        """
        Calculate average processing time in hours.
        
        Args:
            yearmonth: Optional filter by yearmonth (format: YYYYMM)
        """
        where_clause = f"AND yearmonth = '{yearmonth}'" if yearmonth else ""
        
        query = f"""
            SELECT AVG(
                (unix_timestamp(completed_at) - unix_timestamp(submitted_at)) / 3600.0
            ) as avg_hours
            FROM {self.log_manager.full_table_name}
            WHERE completed_at IS NOT NULL 
              AND submitted_at IS NOT NULL
              AND batch_status = 'COMPLETED'
              {where_clause}
        """
        
        result = self.spark.sql(query).collect()
        return result[0]['avg_hours'] if result else 0
    
    def get_batches_by_age(self) -> Dict[str, int]:
        """Get count of batches by age categories."""
        query = f"""
            SELECT 
                CASE 
                    WHEN datediff(current_timestamp(), created_at) = 0 THEN 'Today'
                    WHEN datediff(current_timestamp(), created_at) = 1 THEN 'Yesterday'
                    WHEN datediff(current_timestamp(), created_at) <= 7 THEN 'This Week'
                    WHEN datediff(current_timestamp(), created_at) <= 30 THEN 'This Month'
                    ELSE 'Older'
                END as age_category,
                COUNT(*) as count
            FROM {self.log_manager.full_table_name}
            GROUP BY age_category
            ORDER BY 
                CASE age_category
                    WHEN 'Today' THEN 1
                    WHEN 'Yesterday' THEN 2
                    WHEN 'This Week' THEN 3
                    WHEN 'This Month' THEN 4
                    ELSE 5
                END
        """
        
        result = self.spark.sql(query).collect()
        return {row['age_category']: row['count'] for row in result}
    
    def get_daily_submission_trend(self, days: int = 30) -> List[Dict]:
        """Get daily batch submission trend."""
        query = f"""
            SELECT 
                DATE(created_at) as date,
                COUNT(*) as batches_submitted,
                SUM(total_prompts) as total_prompts
            FROM {self.log_manager.full_table_name}
            WHERE created_at >= date_sub(current_timestamp(), {days})
            GROUP BY DATE(created_at)
            ORDER BY date
        """
        
        result = self.spark.sql(query).collect()
        return [row.asDict() for row in result]
    
    def get_error_summary(self, limit: int = 10) -> List[Dict]:
        """Get most common error messages."""
        query = f"""
            SELECT 
                error_message,
                COUNT(*) as occurrence_count,
                MAX(updated_at) as last_occurrence
            FROM {self.log_manager.full_table_name}
            WHERE error_message IS NOT NULL
            GROUP BY error_message
            ORDER BY occurrence_count DESC
            LIMIT {limit}
        """
        
        result = self.spark.sql(query).collect()
        return [row.asDict() for row in result]
    
    def generate_status_report(self, yearmonth: Optional[str] = None) -> str:
        """
        Generate a comprehensive status report.
        
        Args:
            yearmonth: Optional filter by yearmonth (format: YYYYMM)
        """
        stats = self.log_manager.get_statistics(yearmonth=yearmonth)
        
        report = []
        report.append("=" * 80)
        report.append("BATCH PROCESSING STATUS REPORT")
        if yearmonth:
            report.append(f"Year-Month: {yearmonth}")
        report.append("=" * 80)
        report.append("")
        
        # Overall metrics
        total_batches = sum(s['count'] for s in stats.values())
        total_prompts = sum(s['total_prompts'] or 0 for s in stats.values())
        completed_prompts = sum(s['completed_prompts'] or 0 for s in stats.values())
        
        report.append("OVERALL METRICS")
        report.append("-" * 80)
        report.append(f"Total Batches: {total_batches:,}")
        report.append(f"Total Prompts: {total_prompts:,}")
        report.append(f"Completed Prompts: {completed_prompts:,}")
        report.append(f"Completion Rate: {self.get_completion_rate(yearmonth):.2f}%")
        report.append(f"Failure Rate: {self.get_failure_rate(yearmonth):.2f}%")
        report.append(f"Avg Processing Time: {self.get_average_processing_time(yearmonth):.2f} hours")
        report.append("")
        
        # Status breakdown
        report.append("STATUS BREAKDOWN")
        report.append("-" * 80)
        for status, metrics in sorted(stats.items()):
            report.append(f"\n{status}:")
            report.append(f"  Batches: {metrics['count']:,}")
            report.append(f"  Total Prompts: {metrics['total_prompts']:,}")
            report.append(f"  Completed Prompts: {metrics['completed_prompts']:,}")
            if metrics['failed_prompts']:
                report.append(f"  Failed Prompts: {metrics['failed_prompts']:,}")
        report.append("")
        
        # Age distribution
        report.append("AGE DISTRIBUTION")
        report.append("-" * 80)
        age_dist = self.get_batches_by_age()
        for age, count in age_dist.items():
            report.append(f"{age}: {count:,} batches")
        report.append("")
        
        # Top errors
        report.append("TOP ERRORS")
        report.append("-" * 80)
        errors = self.get_error_summary(limit=5)
        if errors:
            for i, error in enumerate(errors, 1):
                report.append(f"\n{i}. {error['error_message'][:100]}...")
                report.append(f"   Occurrences: {error['occurrence_count']}")
                report.append(f"   Last seen: {error['last_occurrence']}")
        else:
            report.append("No errors recorded")
        
        report.append("\n" + "=" * 80)
        
        return "\n".join(report)
    
    def export_results_summary(self, output_path: str, yearmonth: Optional[str] = None):
        """
        Export a summary of completed batches to CSV.
        
        Args:
            output_path: Output path for CSV file
            yearmonth: Optional filter by yearmonth (format: YYYYMM)
        """
        where_clause = f"AND yearmonth = '{yearmonth}'" if yearmonth else ""
        
        query = f"""
            SELECT 
                batch_id,
                jsonl_file_path,
                batch_status,
                total_prompts,
                completed_prompts,
                failed_prompts,
                model_id,
                output_s3_uri,
                yearmonth,
                DATE(submitted_at) as submission_date,
                DATE(completed_at) as completion_date,
                ROUND(
                    (unix_timestamp(completed_at) - unix_timestamp(submitted_at)) / 3600.0, 
                    2
                ) as processing_hours
            FROM {self.log_manager.full_table_name}
            WHERE batch_status IN ('COMPLETED', 'FAILED')
              {where_clause}
            ORDER BY completed_at DESC
        """
        
        df = self.spark.sql(query)
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
        print(f"✓ Exported results summary to: {output_path}")


# ============================================================================
# MONITORING QUERIES (Direct SQL for Databricks SQL Dashboards)
# ============================================================================

DASHBOARD_QUERIES = {
    "active_batches": """
        -- Active Batches Overview
        SELECT 
            batch_status,
            COUNT(*) as count,
            SUM(total_prompts) as total_prompts,
            SUM(completed_prompts) as completed_prompts,
            ROUND(AVG(retry_count), 2) as avg_retries
        FROM main.default.batch_log_table
        WHERE batch_status IN ('PENDING', 'PROCESSING')
        GROUP BY batch_status
    """,
    
    "completion_trend": """
        -- Daily Completion Trend (Last 30 Days)
        SELECT 
            DATE(completed_at) as date,
            COUNT(*) as batches_completed,
            SUM(total_prompts) as prompts_processed,
            AVG((unix_timestamp(completed_at) - unix_timestamp(submitted_at)) / 3600.0) as avg_hours
        FROM main.default.batch_log_table
        WHERE completed_at >= date_sub(current_date(), 30)
          AND batch_status = 'COMPLETED'
        GROUP BY DATE(completed_at)
        ORDER BY date
    """,
    
    "failure_analysis": """
        -- Failure Analysis
        SELECT 
            error_message,
            COUNT(*) as count,
            MAX(updated_at) as last_occurrence,
            COLLECT_LIST(batch_id) as affected_batches
        FROM main.default.batch_log_table
        WHERE batch_status = 'FAILED'
          AND error_message IS NOT NULL
        GROUP BY error_message
        ORDER BY count DESC
        LIMIT 20
    """,
    
    "processing_efficiency": """
        -- Processing Efficiency Metrics
        SELECT 
            model_id,
            COUNT(*) as total_batches,
            SUM(CASE WHEN batch_status = 'COMPLETED' THEN 1 ELSE 0 END) as completed,
            SUM(CASE WHEN batch_status = 'FAILED' THEN 1 ELSE 0 END) as failed,
            ROUND(AVG(CASE 
                WHEN batch_status = 'COMPLETED' 
                THEN (unix_timestamp(completed_at) - unix_timestamp(submitted_at)) / 3600.0 
            END), 2) as avg_completion_hours,
            ROUND(AVG(retry_count), 2) as avg_retries
        FROM main.default.batch_log_table
        GROUP BY model_id
    """,
    
    "stuck_batches_alert": """
        -- Stuck Batches Alert (Over 24 hours)
        SELECT 
            batch_id,
            batch_status,
            jsonl_file_path,
            created_at,
            updated_at,
            ROUND((unix_timestamp(current_timestamp()) - unix_timestamp(updated_at)) / 3600.0, 1) as hours_stuck,
            retry_count
        FROM main.default.batch_log_table
        WHERE batch_status IN ('PENDING', 'PROCESSING')
          AND updated_at < date_sub(current_timestamp(), 1)
        ORDER BY updated_at ASC
    """
}


# ============================================================================
# EXAMPLE USAGE FOR MONITORING
# ============================================================================

def run_monitoring_report(yearmonth: Optional[str] = None):
    """
    Generate and display monitoring report.
    
    Args:
        yearmonth: Optional filter by yearmonth (format: YYYYMM)
    """
    analytics = BatchAnalytics(catalog="main", schema="default")
    
    # Generate report
    report = analytics.generate_status_report(yearmonth=yearmonth)
    print(report)
    
    # Export results
    output_suffix = f"_{yearmonth}" if yearmonth else ""
    analytics.export_results_summary(
        f"/dbfs/mnt/reports/batch_results_summary{output_suffix}",
        yearmonth=yearmonth
    )
    
    return report


def check_system_health(yearmonth: Optional[str] = None):
    """
    Quick system health check.
    
    Args:
        yearmonth: Optional filter by yearmonth (format: YYYYMM)
    """
    log_manager = BatchLogManager()
    
    # Get stuck batches
    stuck = log_manager.get_stuck_batches(hours_threshold=24, yearmonth=yearmonth)
    
    # Get statistics
    stats = log_manager.get_statistics(yearmonth=yearmonth)
    
    health_status = {
        'yearmonth': yearmonth or BatchLogManager.get_current_yearmonth(),
        'stuck_batches': len(stuck),
        'pending': stats.get('PENDING', {}).get('count', 0),
        'processing': stats.get('PROCESSING', {}).get('count', 0),
        'failed': stats.get('FAILED', {}).get('count', 0),
        'completed': stats.get('COMPLETED', {}).get('count', 0)
    }
    
    # Determine health
    is_healthy = (
        health_status['stuck_batches'] == 0 and
        health_status['failed'] < health_status['completed'] * 0.05  # Less than 5% failure rate
    )
    
    print(f"System Health for {health_status['yearmonth']}: {'✓ HEALTHY' if is_healthy else '✗ ISSUES DETECTED'}")
    print(f"Stuck Batches: {health_status['stuck_batches']}")
    print(f"Active Batches: {health_status['pending'] + health_status['processing']}")
    print(f"Failed Batches: {health_status['failed']}")
    print(f"Completed Batches: {health_status['completed']}")
    
    return health_status, is_healthy
