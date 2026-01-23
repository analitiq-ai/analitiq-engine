# Credentials Management Guide

The Analitiq Stream framework now supports external credentials files for secure authentication, separating sensitive credentials from pipeline configuration.

## Overview

### Benefits
- **Security**: Credentials are stored separately from configuration files
- **Environment Variables**: Support for `${VAR_NAME}` syntax for sensitive values
- **Flexibility**: Different credentials for different environments
- **Validation**: Built-in validation for database and API credentials
- **Templates**: Easy generation of credential file templates

### Architecture
- **CredentialsManager**: Core class for loading and validating credentials
- **Pipeline Integration**: Seamless integration with existing pipeline configurations
- **Environment Expansion**: Automatic expansion of environment variables at runtime

## Usage

### Basic Usage

```python
import json
from src import Pipeline

# Load modular configuration files
with open("pipeline_config.json") as f:
    pipeline_config = json.load(f)
with open("source_config.json") as f:
    source_config = json.load(f)
with open("destination_config.json") as f:
    destination_config = json.load(f)

# Create pipeline with external credentials
pipeline = Pipeline(
    pipeline_config=pipeline_config,
    source_config=source_config,
    destination_config=destination_config,
    source_credentials_path="src_credentials.json",
    destination_credentials_path="dst_credentials.json"
)
```

### Credentials File Formats

#### Database Credentials (`src_credentials.json`)
```json
{
  "host": "localhost",
  "port": 5432,
  "database": "analytics",
  "user": "postgres",
  "password": "${DB_PASSWORD}",
  "driver": "postgresql",
  "ssl_mode": "prefer",
  "connection_timeout": 30,
  "max_connections": 10,
  "min_connections": 1
}
```

#### API Credentials (`dst_credentials.json`)
```json
{
  "base_url": "https://api.example.com",
  "auth": {
    "type": "bearer_token",
    "token": "${API_TOKEN}"
  },
  "headers": {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "AnalitiqStream/1.0"
  },
  "timeout": 30,
  "max_connections": 10,
  "rate_limit": {
    "max_requests": 100,
    "time_window": 60
  }
}
```

**Note**: The `rate_limit` configuration should be defined in API credentials as it applies to the entire API connection rather than individual endpoints.

### Authentication Types

#### API Authentication

**Bearer Token:**
```json
{
  "auth": {
    "type": "bearer_token",
    "token": "${API_TOKEN}"
  }
}
```

**API Key:**
```json
{
  "auth": {
    "type": "api_key",
    "api_key": "${API_KEY}",
    "header_name": "X-API-Key"
  }
}
```

**Basic Authentication:**
```json
{
  "auth": {
    "type": "basic",
    "username": "${API_USERNAME}",
    "password": "${API_PASSWORD}"
  }
}
```

### Environment Variables

Set environment variables for sensitive values:

```bash
export DB_PASSWORD="your_database_password"
export API_TOKEN="your_api_token"
export API_KEY="your_api_key"
```

Variables are expanded using `${VAR_NAME}` syntax in credentials files.

## Template Generation

Generate credential file templates:

```bash
# Generate database credentials template
python examples/generate_credentials_template.py database src_credentials.json

# Generate API credentials template  
python examples/generate_credentials_template.py api dst_credentials.json

# Generate both templates
python examples/generate_credentials_template.py both
```

## Migration from Inline Credentials

### Before (Inline Credentials)
```json
{
  "pipeline_id": "my-pipeline",
  "source": {
    "type": "database",
    "connection": {
      "host": "localhost",
      "user": "postgres",
      "password": "secret123"
    }
  }
}
```

### After (External Credentials)

**pipeline_config.json:**
```json
{
  "pipeline_id": "my-pipeline",
  "source": {
    "type": "database",
    "config": {
      "table": "users"
    }
  }
}
```

**src_credentials.json:**
```json
{
  "host": "localhost",
  "user": "postgres", 
  "password": "${DB_PASSWORD}"
}
```

**Python code:**
```python
pipeline = Pipeline(
    config_path="pipeline_config.json",
    source_credentials_path="src_credentials.json"
)
```

## Security Best Practices

1. **Never commit credentials files** to version control
2. **Use environment variables** for sensitive values
3. **Set appropriate file permissions** (600) for credentials files
4. **Use different credentials** for different environments
5. **Rotate credentials regularly**
6. **Validate credentials** before deployment

### .gitignore Example
```
# Credentials files
*_credentials.json
src_credentials.json
dst_credentials.json
credentials/
```

## Advanced Usage

### Direct CredentialsManager Usage

```python
from src import credentials_manager

# Load and validate credentials
creds = credentials_manager.load_credentials("api_creds.json")
is_valid = credentials_manager.validate_api_credentials(creds)

# Merge with configuration
config = credentials_manager.merge_credentials_with_config(
    pipeline_config["source"], 
    creds
)
```

### Custom Validation

```python
from src import CredentialsManager

# Create custom credentials manager
cm = CredentialsManager()

# Load with custom validation
creds = cm.load_credentials("custom_creds.json")
if not cm.validate_database_credentials(creds):
    raise ValueError("Invalid credentials")
```

## Examples

The framework includes updated examples using credentials files:

- **wise-to-sevdesk**: Uses separate API credentials for Wise and SevDesk
- **basic-pipeline**: Uses database and API credentials files
- **generate_credentials_template.py**: Utility for generating templates

## Troubleshooting

### Common Issues

1. **Environment variable not found**
   - Ensure environment variables are set before running
   - Check variable names match exactly (case-sensitive)

2. **Credentials file not found**
   - Verify file paths are correct
   - Use absolute paths if needed

3. **Validation errors**
   - Check required fields are present
   - Verify authentication type configuration

4. **Permission errors**
   - Ensure credentials files are readable
   - Check file ownership and permissions

### Error Examples

```bash
# Missing environment variable
Environment variable 'API_TOKEN' not found, using original value

# Invalid credentials format
Invalid JSON in credentials file: Expecting ',' delimiter: line 5 column 8

# Missing required field
Missing required API credential field: base_url
```

This credentials management system provides a secure, flexible way to handle authentication while maintaining the simplicity and power of the Analitiq Stream framework.