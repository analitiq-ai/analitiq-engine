# Pipeline Metrics - Athena Setup and Lambda Integration

This document provides instructions for the backend team to set up AWS Athena on the pipeline metrics S3 bucket and create a Lambda function to query the data.

## Overview

Pipeline execution metrics (row counts, duration, status) are stored in S3 with Hive-style partitioning:

```
s3://analitiq-client-pipeline-row-count/
  client_id={client_id}/
    year=2025/
      month=01/
        day=15/
          20250115T103000Z-abc12345.json
```

## Step 1: Create Athena Database

Run this in the Athena Query Editor (or via AWS CLI):

```sql
CREATE DATABASE IF NOT EXISTS analitiq_metrics;
```

## Step 2: Create External Table

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS analitiq_metrics.pipeline_metrics (
  run_id STRING,
  pipeline_id STRING,
  pipeline_name STRING,
  start_time STRING,
  end_time STRING,
  duration_seconds DOUBLE,
  records_processed INT,
  records_failed INT,
  records_total INT,
  batches_processed INT,
  status STRING,
  error_message STRING,
  records_per_second DOUBLE,
  environment STRING
)
PARTITIONED BY (
  client_id STRING,
  year STRING,
  month STRING,
  day STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'true'
)
LOCATION 's3://analitiq-client-pipeline-row-count/'
TBLPROPERTIES (
  'has_encrypted_data' = 'false',
  'projection.enabled' = 'true',
  'projection.client_id.type' = 'injected',
  'projection.year.type' = 'integer',
  'projection.year.range' = '2024,2030',
  'projection.month.type' = 'integer',
  'projection.month.range' = '1,12',
  'projection.month.digits' = '2',
  'projection.day.type' = 'integer',
  'projection.day.range' = '1,31',
  'projection.day.digits' = '2',
  'storage.location.template' = 's3://analitiq-client-pipeline-row-count/client_id=${client_id}/year=${year}/month=${month}/day=${day}'
);
```

**Note:** This uses Athena partition projection which automatically discovers partitions without needing `MSCK REPAIR TABLE`. This is more efficient for frequently updated data.

### Alternative: Manual Partition Discovery

If you prefer manual partition management instead of projection:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS analitiq_metrics.pipeline_metrics (
  run_id STRING,
  pipeline_id STRING,
  pipeline_name STRING,
  start_time STRING,
  end_time STRING,
  duration_seconds DOUBLE,
  records_processed INT,
  records_failed INT,
  records_total INT,
  batches_processed INT,
  status STRING,
  error_message STRING,
  records_per_second DOUBLE,
  environment STRING
)
PARTITIONED BY (
  client_id STRING,
  year STRING,
  month STRING,
  day STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://analitiq-client-pipeline-row-count/'
TBLPROPERTIES ('has_encrypted_data' = 'false');

-- Run this periodically to discover new partitions
MSCK REPAIR TABLE analitiq_metrics.pipeline_metrics;
```

## Step 3: Verify Table Setup

```sql
-- Check table exists
SHOW TABLES IN analitiq_metrics;

-- Preview data
SELECT * FROM analitiq_metrics.pipeline_metrics LIMIT 10;

-- Check partition structure
SHOW PARTITIONS analitiq_metrics.pipeline_metrics;
```

## Step 4: Lambda Function for Querying Metrics

### IAM Role Requirements

The Lambda execution role needs these permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:StopQueryExecution"
      ],
      "Resource": [
        "arn:aws:athena:eu-central-1:*:workgroup/primary"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::analitiq-client-pipeline-row-count",
        "arn:aws:s3:::analitiq-client-pipeline-row-count/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::analitiq-athena-results-*",
        "arn:aws:s3:::analitiq-athena-results-*/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetTable",
        "glue:GetPartitions",
        "glue:GetDatabase"
      ],
      "Resource": [
        "arn:aws:glue:eu-central-1:*:catalog",
        "arn:aws:glue:eu-central-1:*:database/analitiq_metrics",
        "arn:aws:glue:eu-central-1:*:table/analitiq_metrics/*"
      ]
    }
  ]
}
```

### Lambda Function Code (Python 3.11)

```python
"""
Lambda function to query pipeline metrics from Athena.

Environment Variables:
    ATHENA_DATABASE: Database name (default: analitiq_metrics)
    ATHENA_WORKGROUP: Athena workgroup (default: primary)
    ATHENA_OUTPUT_BUCKET: S3 bucket for query results
"""

import json
import os
import time
from datetime import datetime, timedelta
from typing import Any

import boto3
from botocore.exceptions import ClientError


# Configuration
ATHENA_DATABASE = os.environ.get("ATHENA_DATABASE", "analitiq_metrics")
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")
ATHENA_OUTPUT_BUCKET = os.environ.get("ATHENA_OUTPUT_BUCKET", "analitiq-athena-results-dev")

# Initialize clients
athena_client = boto3.client("athena", region_name="eu-central-1")


def execute_athena_query(query: str, timeout_seconds: int = 60) -> list[dict[str, Any]]:
    """
    Execute an Athena query and return results.

    Args:
        query: SQL query to execute
        timeout_seconds: Maximum time to wait for query completion

    Returns:
        List of result rows as dictionaries
    """
    # Start query execution
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={
            "OutputLocation": f"s3://{ATHENA_OUTPUT_BUCKET}/query-results/"
        }
    )

    query_execution_id = response["QueryExecutionId"]

    # Wait for query to complete
    start_time = time.time()
    while True:
        status_response = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        status = status_response["QueryExecution"]["Status"]["State"]

        if status == "SUCCEEDED":
            break
        elif status in ("FAILED", "CANCELLED"):
            reason = status_response["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown error"
            )
            raise RuntimeError(f"Query {status}: {reason}")

        if time.time() - start_time > timeout_seconds:
            athena_client.stop_query_execution(QueryExecutionId=query_execution_id)
            raise TimeoutError(f"Query timed out after {timeout_seconds} seconds")

        time.sleep(0.5)

    # Fetch results
    results = []
    paginator = athena_client.get_paginator("get_query_results")

    for page in paginator.paginate(QueryExecutionId=query_execution_id):
        rows = page["ResultSet"]["Rows"]

        # First row contains column headers
        if not results and rows:
            headers = [col["VarCharValue"] for col in rows[0]["Data"]]
            rows = rows[1:]

        for row in rows:
            values = [col.get("VarCharValue") for col in row["Data"]]
            results.append(dict(zip(headers, values)))

    return results


def get_daily_metrics(client_id: str, start_date: str, end_date: str) -> list[dict]:
    """
    Get aggregated metrics by day for a client over a date range.

    Args:
        client_id: Client UUID
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        List of daily aggregated metrics
    """
    # Parse dates for partition filtering
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    query = f"""
    SELECT
        year,
        month,
        day,
        COUNT(*) as pipeline_runs,
        SUM(records_processed) as total_records_processed,
        SUM(records_failed) as total_records_failed,
        SUM(records_total) as total_records,
        AVG(duration_seconds) as avg_duration_seconds,
        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_runs,
        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_runs,
        SUM(CASE WHEN status = 'partial' THEN 1 ELSE 0 END) as partial_runs
    FROM pipeline_metrics
    WHERE client_id = '{client_id}'
      AND CAST(CONCAT(year, '-', month, '-', day) AS DATE)
          BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    GROUP BY year, month, day
    ORDER BY year, month, day
    """

    return execute_athena_query(query)


def get_pipeline_metrics(
    client_id: str,
    pipeline_id: str,
    start_date: str,
    end_date: str
) -> list[dict]:
    """
    Get detailed metrics for a specific pipeline over a date range.

    Args:
        client_id: Client UUID
        pipeline_id: Pipeline UUID
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        List of pipeline run metrics
    """
    query = f"""
    SELECT
        run_id,
        pipeline_id,
        pipeline_name,
        start_time,
        end_time,
        duration_seconds,
        records_processed,
        records_failed,
        records_total,
        batches_processed,
        status,
        error_message,
        records_per_second,
        year,
        month,
        day
    FROM pipeline_metrics
    WHERE client_id = '{client_id}'
      AND pipeline_id = '{pipeline_id}'
      AND CAST(CONCAT(year, '-', month, '-', day) AS DATE)
          BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    ORDER BY start_time DESC
    """

    return execute_athena_query(query)


def get_pipeline_summary(client_id: str, year: str) -> list[dict]:
    """
    Get summary metrics for all pipelines for a client in a year.

    Args:
        client_id: Client UUID
        year: Year (YYYY)

    Returns:
        List of pipeline summaries
    """
    query = f"""
    SELECT
        pipeline_id,
        pipeline_name,
        COUNT(*) as total_runs,
        SUM(records_processed) as total_records_processed,
        SUM(records_failed) as total_records_failed,
        AVG(records_per_second) as avg_throughput,
        AVG(duration_seconds) as avg_duration_seconds,
        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_runs,
        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_runs,
        MIN(start_time) as first_run,
        MAX(start_time) as last_run
    FROM pipeline_metrics
    WHERE client_id = '{client_id}'
      AND year = '{year}'
    GROUP BY pipeline_id, pipeline_name
    ORDER BY total_records_processed DESC
    """

    return execute_athena_query(query)


def lambda_handler(event: dict, context: Any) -> dict:
    """
    Lambda handler for pipeline metrics queries.

    Event structure:
    {
        "action": "daily_metrics" | "pipeline_metrics" | "pipeline_summary",
        "client_id": "uuid",
        "pipeline_id": "uuid",  // required for pipeline_metrics
        "start_date": "YYYY-MM-DD",
        "end_date": "YYYY-MM-DD",
        "year": "YYYY"  // required for pipeline_summary
    }
    """
    try:
        action = event.get("action")
        client_id = event.get("client_id")

        if not client_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "client_id is required"})
            }

        if action == "daily_metrics":
            start_date = event.get("start_date")
            end_date = event.get("end_date")

            if not start_date or not end_date:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": "start_date and end_date are required"})
                }

            results = get_daily_metrics(client_id, start_date, end_date)

        elif action == "pipeline_metrics":
            pipeline_id = event.get("pipeline_id")
            start_date = event.get("start_date")
            end_date = event.get("end_date")

            if not pipeline_id:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": "pipeline_id is required"})
                }
            if not start_date or not end_date:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": "start_date and end_date are required"})
                }

            results = get_pipeline_metrics(client_id, pipeline_id, start_date, end_date)

        elif action == "pipeline_summary":
            year = event.get("year", str(datetime.now().year))
            results = get_pipeline_summary(client_id, year)

        else:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "error": f"Unknown action: {action}",
                    "valid_actions": ["daily_metrics", "pipeline_metrics", "pipeline_summary"]
                })
            }

        return {
            "statusCode": 200,
            "body": json.dumps({
                "data": results,
                "count": len(results)
            })
        }

    except TimeoutError as e:
        return {
            "statusCode": 504,
            "body": json.dumps({"error": str(e)})
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
```

### Lambda Configuration

| Setting | Value |
|---------|-------|
| Runtime | Python 3.11 |
| Memory | 256 MB |
| Timeout | 120 seconds |
| Architecture | x86_64 or arm64 |

### Environment Variables for Lambda

| Variable | Value | Description |
|----------|-------|-------------|
| `ATHENA_DATABASE` | `analitiq_metrics` | Athena database name |
| `ATHENA_WORKGROUP` | `primary` | Athena workgroup |
| `ATHENA_OUTPUT_BUCKET` | `analitiq-athena-results-dev` | S3 bucket for query results |

## Step 5: Create Athena Workgroup (Optional)

For better cost tracking and query limits:

```bash
aws athena create-work-group \
    --name analitiq-metrics \
    --configuration '{
        "ResultConfiguration": {
            "OutputLocation": "s3://analitiq-athena-results-dev/workgroup-results/"
        },
        "EnforceWorkGroupConfiguration": true,
        "PublishCloudWatchMetricsEnabled": true,
        "BytesScannedCutoffPerQuery": 10737418240
    }' \
    --region eu-central-1
```

## Step 6: API Gateway Integration (Optional)

To expose the Lambda via REST API:

1. Create a REST API in API Gateway
2. Create resources: `/metrics/daily`, `/metrics/pipeline`, `/metrics/summary`
3. Create POST methods linked to the Lambda
4. Enable CORS if needed
5. Deploy to a stage (e.g., `dev`, `prod`)

### Example API Requests

**Get daily metrics:**
```bash
curl -X POST https://api.example.com/metrics/daily \
  -H "Content-Type: application/json" \
  -d '{
    "action": "daily_metrics",
    "client_id": "d7a11991-2795-49d1-a858-c7e58ee5ecc6",
    "start_date": "2025-01-01",
    "end_date": "2025-01-31"
  }'
```

**Get pipeline metrics:**
```bash
curl -X POST https://api.example.com/metrics/pipeline \
  -H "Content-Type: application/json" \
  -d '{
    "action": "pipeline_metrics",
    "client_id": "d7a11991-2795-49d1-a858-c7e58ee5ecc6",
    "pipeline_id": "7c4cc24f-7208-4005-abad-8c0f2456714f",
    "start_date": "2025-01-01",
    "end_date": "2025-01-31"
  }'
```

**Get pipeline summary:**
```bash
curl -X POST https://api.example.com/metrics/summary \
  -H "Content-Type: application/json" \
  -d '{
    "action": "pipeline_summary",
    "client_id": "d7a11991-2795-49d1-a858-c7e58ee5ecc6",
    "year": "2025"
  }'
```

## Troubleshooting

### Common Issues

1. **"Table not found" error**
   - Verify the database and table exist: `SHOW TABLES IN analitiq_metrics`
   - Check the S3 bucket location is correct

2. **"Access Denied" error**
   - Verify Lambda IAM role has permissions for S3, Athena, and Glue
   - Check the Athena results bucket is accessible

3. **No data returned**
   - Check partitions exist: `SHOW PARTITIONS analitiq_metrics.pipeline_metrics`
   - Verify data exists in S3: `aws s3 ls s3://analitiq-client-pipeline-row-count/ --recursive`
   - If using manual partitions, run `MSCK REPAIR TABLE`

4. **Query timeout**
   - Increase Lambda timeout
   - Add partition filtering to reduce data scanned
   - Consider using partition projection (shown in Step 2)

### Useful Debug Queries

```sql
-- Check what clients have data
SELECT DISTINCT client_id FROM analitiq_metrics.pipeline_metrics;

-- Check date range of available data
SELECT
    MIN(CONCAT(year, '-', month, '-', day)) as earliest_date,
    MAX(CONCAT(year, '-', month, '-', day)) as latest_date,
    COUNT(*) as total_records
FROM analitiq_metrics.pipeline_metrics
WHERE client_id = 'your-client-id';

-- Check data for specific date
SELECT *
FROM analitiq_metrics.pipeline_metrics
WHERE client_id = 'your-client-id'
  AND year = '2025'
  AND month = '01'
  AND day = '15';
```
