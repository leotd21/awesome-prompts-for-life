Design a solution for building a FeatureStore on Databricks using Medallion architecture (bronze/silver/gold) without native Databricks Feature Store.
Requirements:
- Daily data ingestion from S3 (new partition_date appended daily)
- Automated data curation pipeline: bronze → silver → gold layers
- Gold layer features feed a pre-built model stored in Databricks Volumes
- Daily batch inference job on new data
Provide recommendations on:
1. Pipeline orchestration strategy
2. Technology stack and tools
3. Deployment architecture
4. Compute resources (cluster types, sizing)
5. Cost optimization approaches
