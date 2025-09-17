# Docker Deployment for Analitiq Stream

This directory contains Docker configuration files for deploying Analitiq Stream pipelines in containers, with configurations that mirror AWS ECS deployments.

## Files Overview

- `Dockerfile` - Multi-stage Docker build for the application
- `docker-compose.yml` - Local development and testing configuration
- `docker-compose.aws.yml` - AWS ECS-compatible configuration
- `entrypoint.py` - Container entrypoint script for pipeline execution
- `.env.example` - Example environment variables configuration

## Quick Start

### 1. Local Development

```bash
# Copy environment template
cp docker/.env.example docker/.env

# Edit environment variables
nano docker/.env

# Build and run locally
cd docker
docker-compose up --build
```

### 2. AWS ECS Simulation

```bash
# Use AWS-compatible configuration
cd docker
docker-compose -f docker-compose.aws.yml up --build
```

## Configuration

### Environment Variables

The container requires these essential environment variables:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `ENV` | Yes | `local` | Deployment environment: `local`, `dev`, `prod` |
| `PIPELINE_ID` | Yes | - | UUID of the pipeline to execute |
| `AWS_REGION` | No | `eu-central-1` | AWS region for all services |
| `S3_CONFIG_BUCKET` | No | `analitiq-config` | S3 bucket for configuration files |
| `LOCAL_CONFIG_MOUNT` | No | `/config` | Local mount point for configs |

### Configuration Sources

#### Local Mode (`ENV=local`)
- Configurations loaded from mounted volume at `/config`
- Directory structure: `/config/pipelines/`, `/config/hosts/`, `/config/endpoints/`
- Secrets loaded from environment variables

#### AWS Mode (`ENV=dev` or `ENV=prod`)
- Configurations loaded from S3: `s3://bucket/pipelines/`, `s3://bucket/hosts/`, `s3://bucket/endpoints/`
- Secrets loaded from environment variables
- AWS credentials from IAM roles (in ECS) or environment variables

## Resource Configuration

The Docker Compose configurations include resource limits that mirror ECS task definitions:

- **CPU**: 0.5-1.0 cores (512-1024 CPU units in ECS)
- **Memory**: 1-2 GB RAM
- **Storage**: Persistent volumes for state, logs, and dead letter queue

## Deployment Patterns

### Local Testing
```bash
# Test with local configurations
ENV=local PIPELINE_ID=your-pipeline-id docker-compose up
```

### Development Environment
```bash
# Test with S3 configurations
ENV=dev PIPELINE_ID=your-pipeline-id docker-compose -f docker-compose.aws.yml up
```

### Production Deployment

For production ECS deployment, the `docker-compose.aws.yml` serves as a reference. Create an ECS task definition with:

1. **Container Definition**:
   - Image: `analitiq-stream:latest`
   - CPU: 1024 units
   - Memory: 2048 MB
   - Environment variables as shown in `docker-compose.aws.yml`

2. **IAM Role**: Attach policy with permissions for:
   - S3: Read access to config bucket
   - CloudWatch: Write access for logging

3. **VPC Configuration**: Deploy in private subnets with NAT gateway for external API access

4. **Storage**: Use EFS volumes for persistent state if needed

## Health Checks

The container includes health checks that verify:
- Python environment is working
- Core modules can be imported
- Basic configuration validation

Health check endpoint: Container runs internal validation every 30 seconds.

## Logging

- **Local**: JSON file logs with rotation
- **AWS**: CloudWatch logs integration
- **Format**: Structured JSON logs with timestamps, levels, and context

## Troubleshooting

### Common Issues

1. **Configuration Not Found**
   - Verify `PIPELINE_ID` exists in the specified config source
   - Check mount paths for local mode
   - Verify S3 bucket access for AWS mode

2. **AWS Credentials**
   - Ensure IAM roles are properly configured for ECS
   - For local AWS testing, verify AWS credentials are set

3. **Network Connectivity**
   - Verify external API endpoints are accessible
   - Check security groups and NAT gateway configuration for ECS

### Debug Mode

Run with debug logging:
```bash
docker-compose run --rm -e LOG_LEVEL=DEBUG analitiq-stream
```

## Security Considerations

- Container runs as non-root user (`appuser`)
- Secrets should be passed via environment variables in ECS task definitions
- Use IAM roles instead of access keys in ECS
- Network policies should restrict outbound connections to required services

## Volume Management

Persistent data is stored in named volumes:
- `analitiq_state`: Pipeline state and checkpoints
- `analitiq_logs`: Application logs
- `analitiq_deadletter`: Failed records for manual review

In ECS, these would typically map to EFS volumes for shared access across tasks.