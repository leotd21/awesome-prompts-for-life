-- Batch Log Table Schema
CREATE TABLE IF NOT EXISTS batch_log_table (
  batch_id STRING NOT NULL,
  jsonl_file_path STRING NOT NULL,
  batch_status STRING NOT NULL,
  total_prompts INT,
  completed_prompts INT,
  failed_prompts INT,
  
  -- Timestamps
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  submitted_at TIMESTAMP,
  completed_at TIMESTAMP,
  
  -- AWS Bedrock specific metadata
  model_id STRING,
  input_s3_uri STRING,
  output_s3_uri STRING,
  
  -- Error tracking
  error_message STRING,
  retry_count INT DEFAULT 0,
  max_retries INT DEFAULT 3,
  
  -- Job tracking
  job_name STRING,
  job_run_id STRING,
  
  -- Partition columns for performance
  created_date DATE GENERATED ALWAYS AS (CAST(created_at AS DATE)),
  yearmonth STRING NOT NULL
)
USING DELTA
PARTITIONED BY (yearmonth, batch_status)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Tracks AWS Bedrock batch processing jobs with status and metadata';

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_batch_status ON batch_log_table(batch_status);
CREATE INDEX IF NOT EXISTS idx_created_at ON batch_log_table(created_at);

-- Add primary key constraint (Delta Lake supports this)
ALTER TABLE batch_log_table 
ADD CONSTRAINT batch_log_pk PRIMARY KEY(batch_id);
