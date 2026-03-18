# Pipeline Metrics - CloudWatch Logs Integration

This document provides instructions for querying pipeline metrics from CloudWatch Logs.

## Overview

Pipeline execution metrics are emitted as structured JSON logs with a marker prefix for easy extraction:

```
ANALITIQ_METRICS::{"type":"pipeline","run_id":"abc123",...}
```

### Metrics Types

**Batch metrics** (emitted per batch in `engine.py`):
```json
ANALITIQ_METRICS::{"type":"batch","run_id":"abc123","pipeline_id":"xyz","stream_id":"s1","batch_seq":1,"records_written":1000,"cumulative_records_processed":1000,"cumulative_records_failed":0,"cumulative_batches_processed":1,"timestamp":"2025-02-04T10:30:15+00:00"}
```

**Pipeline metrics** (emitted at pipeline completion in `runner.py`):
```json
ANALITIQ_METRICS::{"type":"pipeline","run_id":"abc123","pipeline_id":"xyz","pipeline_name":"Sales Sync","org_id":"d7a11991-2795-49d1-a858-c7e58ee5ecc6","start_time":"2025-02-04T10:00:00+00:00","end_time":"2025-02-04T10:35:00+00:00","duration_seconds":2100.5,"records_processed":15000,"records_failed":0,"records_total":15000,"batches_processed":15,"status":"success","error_message":null,"records_per_second":7.14,"environment":"prod"}
```

## CloudWatch Logs Insights Queries

### Basic Extraction

Extract all metrics from a log group:

```sql
fields @timestamp, @message
| filter @message like /ANALITIQ_METRICS::/
| parse @message /ANALITIQ_METRICS::(?<metrics>.*)$/
| limit 100
```

### Pipeline Completion Metrics

Get all pipeline completion metrics:

```sql
fields @timestamp
| filter @message like /ANALITIQ_METRICS::/
| parse @message /ANALITIQ_METRICS::(?<metrics>.*)$/
| filter metrics like /"type":"pipeline"/
| parse metrics /"run_id":"(?<run_id>[^"]+)"/
| parse metrics /"pipeline_id":"(?<pipeline_id>[^"]+)"/
| parse metrics /"pipeline_name":"(?<pipeline_name>[^"]+)"/
| parse metrics /"records_processed":(?<records_processed>\d+)/
| parse metrics /"records_failed":(?<records_failed>\d+)/
| parse metrics /"duration_seconds":(?<duration_seconds>[\d.]+)/
| parse metrics /"status":"(?<status>[^"]+)"/
| sort @timestamp desc
| limit 100
```

### Filter by Org ID

```sql
fields @timestamp
| filter @message like /ANALITIQ_METRICS::/
| parse @message /ANALITIQ_METRICS::(?<metrics>.*)$/
| filter metrics like /"org_id":"d7a11991-2795-49d1-a858-c7e58ee5ecc6"/
| filter metrics like /"type":"pipeline"/
| parse metrics /"pipeline_name":"(?<pipeline_name>[^"]+)"/
| parse metrics /"records_processed":(?<records_processed>\d+)/
| parse metrics /"status":"(?<status>[^"]+)"/
| sort @timestamp desc
```

### Filter by Pipeline ID

```sql
fields @timestamp
| filter @message like /ANALITIQ_METRICS::/
| parse @message /ANALITIQ_METRICS::(?<metrics>.*)$/
| filter metrics like /"pipeline_id":"7c4cc24f-7208-4005-abad-8c0f2456714f"/
| filter metrics like /"type":"pipeline"/
| parse metrics /"run_id":"(?<run_id>[^"]+)"/
| parse metrics /"records_processed":(?<records_processed>\d+)/
| parse metrics /"duration_seconds":(?<duration_seconds>[\d.]+)/
| parse metrics /"status":"(?<status>[^"]+)"/
| sort @timestamp desc
```

### Aggregate Daily Metrics

```sql
fields @timestamp
| filter @message like /ANALITIQ_METRICS::/
| parse @message /ANALITIQ_METRICS::(?<metrics>.*)$/
| filter metrics like /"type":"pipeline"/
| filter metrics like /"org_id":"d7a11991-2795-49d1-a858-c7e58ee5ecc6"/
| parse metrics /"records_processed":(?<records_processed>\d+)/
| parse metrics /"records_failed":(?<records_failed>\d+)/
| stats sum(records_processed) as total_processed, sum(records_failed) as total_failed, count() as pipeline_runs by bin(1d)
| sort @timestamp
```

### Batch-Level Progress Tracking

Track batch progress for a specific run:

```sql
fields @timestamp
| filter @message like /ANALITIQ_METRICS::/
| parse @message /ANALITIQ_METRICS::(?<metrics>.*)$/
| filter metrics like /"type":"batch"/
| filter metrics like /"run_id":"abc123"/
| parse metrics /"stream_id":"(?<stream_id>[^"]+)"/
| parse metrics /"batch_seq":(?<batch_seq>\d+)/
| parse metrics /"records_written":(?<records_written>\d+)/
| parse metrics /"cumulative_records_processed":(?<cumulative>\d+)/
| sort @timestamp asc
```

### Failed Pipelines

```sql
fields @timestamp
| filter @message like /ANALITIQ_METRICS::/
| parse @message /ANALITIQ_METRICS::(?<metrics>.*)$/
| filter metrics like /"type":"pipeline"/
| filter metrics like /"status":"failed"/
| parse metrics /"run_id":"(?<run_id>[^"]+)"/
| parse metrics /"pipeline_name":"(?<pipeline_name>[^"]+)"/
| parse metrics /"error_message":"(?<error_message>[^"]+)"/
| sort @timestamp desc
```

### Throughput Analysis

```sql
fields @timestamp
| filter @message like /ANALITIQ_METRICS::/
| parse @message /ANALITIQ_METRICS::(?<metrics>.*)$/
| filter metrics like /"type":"pipeline"/
| filter metrics like /"status":"success"/
| parse metrics /"pipeline_name":"(?<pipeline_name>[^"]+)"/
| parse metrics /"records_per_second":(?<throughput>[\d.]+)/
| parse metrics /"duration_seconds":(?<duration>[\d.]+)/
| stats avg(throughput) as avg_throughput, max(throughput) as max_throughput, avg(duration) as avg_duration by pipeline_name
```

## Lambda Function for Querying Metrics

### IAM Role Requirements

The Lambda execution role needs these permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "logs:StartQuery",
        "logs:GetQueryResults",
        "logs:StopQuery",
        "logs:DescribeLogGroups"
      ],
      "Resource": [
        "arn:aws:logs:eu-central-1:*:log-group:/aws/batch/*",
        "arn:aws:logs:eu-central-1:*:log-group:/ecs/*"
      ]
    }
  ]
}
```

### Lambda Function Code (Python 3.11)

```python
"""
Lambda function to query pipeline metrics from CloudWatch Logs.

Environment Variables:
    LOG_GROUP_NAME: CloudWatch log group name
    AWS_REGION: AWS region (default: eu-central-1)
"""

import json
import os
import time
from datetime import datetime, timedelta
from typing import Any

import boto3


LOG_GROUP_NAME = os.environ.get("LOG_GROUP_NAME", "/aws/batch/job")
AWS_REGION = os.environ.get("AWS_REGION", "eu-central-1")

logs_client = boto3.client("logs", region_name=AWS_REGION)


def execute_logs_query(query: str, start_time: datetime, end_time: datetime, timeout_seconds: int = 60) -> list[dict]:
    """
    Execute a CloudWatch Logs Insights query and return results.
    """
    response = logs_client.start_query(
        logGroupName=LOG_GROUP_NAME,
        startTime=int(start_time.timestamp()),
        endTime=int(end_time.timestamp()),
        queryString=query,
    )

    query_id = response["queryId"]

    start = time.time()
    while True:
        result = logs_client.get_query_results(queryId=query_id)
        status = result["status"]

        if status == "Complete":
            break
        elif status in ("Failed", "Cancelled"):
            raise RuntimeError(f"Query {status}")

        if time.time() - start > timeout_seconds:
            logs_client.stop_query(queryId=query_id)
            raise TimeoutError(f"Query timed out after {timeout_seconds} seconds")

        time.sleep(0.5)

    # Convert results to list of dicts
    results = []
    for row in result["results"]:
        record = {field["field"]: field["value"] for field in row if not field["field"].startswith("@")}
        results.append(record)

    return results


def get_pipeline_metrics(org_id: str, start_date: str, end_date: str, pipeline_id: str = None) -> list[dict]:
    """
    Get pipeline completion metrics for a client.
    """
    start_time = datetime.strptime(start_date, "%Y-%m-%d")
    end_time = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)

    pipeline_filter = ""
    if pipeline_id:
        pipeline_filter = f'| filter metrics like /"pipeline_id":"{pipeline_id}"/'

    query = f"""
    fields @timestamp
    | filter @message like /ANALITIQ_METRICS::/
    | parse @message /ANALITIQ_METRICS::(?<metrics>.*)$/
    | filter metrics like /"type":"pipeline"/
    | filter metrics like /"org_id":"{org_id}"/
    {pipeline_filter}
    | parse metrics /"run_id":"(?<run_id>[^"]+)"/
    | parse metrics /"pipeline_id":"(?<pipeline_id>[^"]+)"/
    | parse metrics /"pipeline_name":"(?<pipeline_name>[^"]+)"/
    | parse metrics /"records_processed":(?<records_processed>\\d+)/
    | parse metrics /"records_failed":(?<records_failed>\\d+)/
    | parse metrics /"duration_seconds":(?<duration_seconds>[\\d.]+)/
    | parse metrics /"status":"(?<status>[^"]+)"/
    | sort @timestamp desc
    | limit 1000
    """

    return execute_logs_query(query, start_time, end_time)


def get_daily_aggregates(org_id: str, start_date: str, end_date: str) -> list[dict]:
    """
    Get daily aggregated metrics for a client.
    """
    start_time = datetime.strptime(start_date, "%Y-%m-%d")
    end_time = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)

    query = f"""
    fields @timestamp
    | filter @message like /ANALITIQ_METRICS::/
    | parse @message /ANALITIQ_METRICS::(?<metrics>.*)$/
    | filter metrics like /"type":"pipeline"/
    | filter metrics like /"org_id":"{org_id}"/
    | parse metrics /"records_processed":(?<records_processed>\\d+)/
    | parse metrics /"records_failed":(?<records_failed>\\d+)/
    | stats sum(records_processed) as total_processed, sum(records_failed) as total_failed, count() as pipeline_runs by bin(1d) as day
    | sort day
    """

    return execute_logs_query(query, start_time, end_time)


def lambda_handler(event: dict, context: Any) -> dict:
    """
    Lambda handler for pipeline metrics queries.

    Event structure:
    {
        "action": "pipeline_metrics" | "daily_aggregates",
        "org_id": "uuid",
        "pipeline_id": "uuid",  // optional
        "start_date": "YYYY-MM-DD",
        "end_date": "YYYY-MM-DD"
    }
    """
    try:
        action = event.get("action")
        org_id = event.get("org_id")
        start_date = event.get("start_date")
        end_date = event.get("end_date")

        if not org_id:
            return {"statusCode": 400, "body": json.dumps({"error": "org_id is required"})}
        if not start_date or not end_date:
            return {"statusCode": 400, "body": json.dumps({"error": "start_date and end_date are required"})}

        if action == "pipeline_metrics":
            pipeline_id = event.get("pipeline_id")
            results = get_pipeline_metrics(org_id, start_date, end_date, pipeline_id)
        elif action == "daily_aggregates":
            results = get_daily_aggregates(org_id, start_date, end_date)
        else:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "error": f"Unknown action: {action}",
                    "valid_actions": ["pipeline_metrics", "daily_aggregates"]
                })
            }

        return {"statusCode": 200, "body": json.dumps({"data": results, "count": len(results)})}

    except TimeoutError as e:
        return {"statusCode": 504, "body": json.dumps({"error": str(e)})}
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
```

### Lambda Configuration

| Setting | Value |
|---------|-------|
| Runtime | Python 3.11 |
| Memory | 256 MB |
| Timeout | 120 seconds |

### Environment Variables

| Variable | Value | Description |
|----------|-------|-------------|
| `LOG_GROUP_NAME` | `/aws/batch/job` | CloudWatch log group |
| `AWS_REGION` | `eu-central-1` | AWS region |

## Metrics Schema Reference

### Pipeline Metrics Fields

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | Always `"pipeline"` for completion metrics |
| `run_id` | string | Unique run identifier |
| `pipeline_id` | string | Pipeline UUID |
| `pipeline_name` | string | Human-readable pipeline name |
| `org_id` | string | Org UUID |
| `start_time` | string | ISO 8601 start timestamp |
| `end_time` | string | ISO 8601 end timestamp |
| `duration_seconds` | float | Total execution duration |
| `records_processed` | int | Successfully processed records |
| `records_failed` | int | Failed records |
| `records_total` | int | Total records attempted |
| `batches_processed` | int | Number of batches |
| `status` | string | `success`, `failed`, or `partial` |
| `error_message` | string | Error details if failed |
| `records_per_second` | float | Processing throughput |
| `environment` | string | `local`, `dev`, or `prod` |

### Batch Metrics Fields

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | Always `"batch"` for batch metrics |
| `run_id` | string | Unique run identifier |
| `pipeline_id` | string | Pipeline UUID |
| `stream_id` | string | Stream UUID |
| `batch_seq` | int | Batch sequence number |
| `records_written` | int | Records written in this batch |
| `cumulative_records_processed` | int | Total processed so far |
| `cumulative_records_failed` | int | Total failed so far |
| `cumulative_batches_processed` | int | Total batches so far |
| `timestamp` | string | ISO 8601 timestamp |

## Migration from S3-Based Metrics

The previous implementation stored metrics as JSON files in S3:
```
s3://analitiq-client-pipeline-row-count/org_id={id}/year=.../month=.../day=.../{run_id}.json
```

The new implementation emits metrics directly to logs, which:
- Eliminates S3 write latency and costs
- Provides real-time visibility via CloudWatch
- Simplifies the codebase (no S3 client setup needed)
- Enables streaming batch-level progress tracking

To query historical S3 data alongside new log data, you may need to maintain the Athena table for a transition period.
