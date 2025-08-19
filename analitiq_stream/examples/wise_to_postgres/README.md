# Wise to SevDesk Transaction Sync

This example demonstrates how to synchronize transaction data from Wise.com API to SevDesk bank account transactions using the Analitiq Stream framework.

## Overview

The pipeline streams transaction data from Wise.com and transforms it into SevDesk bank account transactions, handling:
- Incremental sync based on transaction creation date
- Field mapping between Wise and SevDesk schemas
- Rate limiting for both APIs
- Error handling with dead letter queue
- Transaction validation and transformation

## Files

- `pipeline_config.json` - Complete pipeline configuration with transformations and mappings
- `src_credentials.json` - Wise API credentials (secure)
- `dst_credentials.json` - SevDesk API credentials (secure)
- `pipeline.py` - Executable pipeline script with monitoring
- `README.md` - This documentation

## Setup

### 1. Credentials Files

The pipeline now uses separate credentials files for security. You need two files:

**src_credentials.json** (Wise API credentials):
```json
{
  "base_url": "https://api.wise.com",
  "auth": {
    "type": "bearer_token",
    "token": "${WISE_API_TOKEN}"
  },
  "headers": {
    "Content-Type": "application/json",
    "Accept": "application/json"
  }
}
```

**dst_credentials.json** (SevDesk API credentials):
```json
{
  "base_url": "https://my.sevdesk.de/api/v1",
  "auth": {
    "type": "api_key",
    "api_key": "${SEVDESK_API_TOKEN}",
    "header_name": "Authorization"
  },
  "headers": {
    "Content-Type": "application/json",
    "Accept": "application/json"
  }
}
```

### 2. Environment Variables

Set the following environment variables:

```bash
export WISE_API_TOKEN="your_wise_api_token"
export SEVDESK_API_TOKEN="your_sevdesk_api_token"
export SEVDESK_BANK_ACCOUNT_ID="your_sevdesk_bank_account_id"
export WISE_PROFILE_ID="your_wise_profile_id"
export WISE_ACCOUNT_ID="your_wise_account_id"
```

### 3. API Setup

**Wise API:**
- Obtain API token from Wise developer portal
- Get your profile ID and account ID from your Wise account
- Ensure API token has read permissions for transactions

**SevDesk API:**
- Obtain API token from SevDesk settings
- Get your bank account ID from SevDesk chart of accounts
- Ensure API token has write permissions for transactions

### 4. Generate Credentials Templates

You can generate template credentials files using the provided script:

```bash
# Generate both source and destination credential templates
python ../generate_credentials_template.py both

# Or generate them individually
python ../generate_credentials_template.py api src_credentials.json
python ../generate_credentials_template.py api dst_credentials.json
```

### 5. Install Dependencies

```bash
poetry install
```

## Usage

### Basic Sync
```bash
python pipeline.py sync
```

### With Monitoring
```bash
python pipeline.py monitor
```

### Incremental Sync (Last 7 Days)
```bash
python pipeline.py incremental
```

### Validate Environment
```bash
python pipeline.py validate
```

## Configuration

### Consolidated Pipeline Configuration

The `pipeline_config.json` now contains everything in a single file:
- **Pipeline Metadata**: ID, name, description, version
- **Source Configuration**: Wise API setup with fields, filters, and pagination
- **Destination Configuration**: SevDesk API setup with endpoints and upsert strategy
- **Transformations**: Complete field mapping and data transformation rules
- **Validation Rules**: Data quality validation with error handling
- **Error Handling**: Dead letter queue and retry logic with categorization
- **Monitoring**: Metrics, logging, and health check configuration
- **Scheduling**: Automated execution configuration

This consolidation eliminates duplication and provides a single source of truth for the entire pipeline configuration.

### Field Mapping

Key mappings from Wise to SevDesk:
- `id` → `externalId`
- `amount` → `amount`
- `currency` → `currency`
- `createdAt` → `valueDate`
- `description` → `purpose`
- `details.merchant.name` → `payeeName`

### Transformations

- Date format conversion (ISO to SevDesk format)
- Amount sign handling for debit/credit
- Transaction status mapping
- Category assignment based on merchant data
- Computed fields for tracking and audit

## Monitoring

The pipeline provides:
- Real-time progress monitoring
- Comprehensive logging to file and console
- Metrics tracking (records processed, failed, etc.)
- Error categorization and handling
- Dead letter queue for failed records

## Error Handling

- **Validation Errors**: Sent to dead letter queue
- **API Errors**: Retried with exponential backoff
- **Rate Limiting**: Automatic throttling and retry
- **Network Issues**: Circuit breaker pattern

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Verify API tokens are valid and have correct permissions
   - Check token expiration dates

2. **Rate Limiting**
   - Pipeline automatically handles rate limits
   - Adjust `requests_per_second` in config if needed

3. **Field Mapping Errors**
   - Check dead letter queue for failed records
   - Verify field names match API responses

4. **Date Format Issues**
   - Ensure date transformations are working correctly
   - Check timezone handling

### Logs

- Pipeline logs to `wise_to_sevdesk.log`
- Dead letter queue records stored in `deadletter/wise_to_sevdesk/`
- State checkpoints in `state/wise_to_sevdesk_state.json`

## Scheduling

For production use, schedule the pipeline with:
- **Cron**: Every hour or daily
- **Systemd**: As a service with timers
- **Docker**: Containerized with orchestration
- **Cloud**: AWS Lambda, Azure Functions, etc.

Example cron entry:
```bash
# Run every hour
0 * * * * /path/to/pipeline.py sync >> /var/log/wise-sevdesk.log 2>&1
```