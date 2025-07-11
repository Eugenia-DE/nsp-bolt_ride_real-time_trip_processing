# Real-Time Trip Analytics Pipeline

A serverless, event-driven data pipeline for ride-hailing platforms built on AWS. This project demonstrates real-time data ingestion, processing, and analytics using modern cloud architecture patterns.

## Project Overview

This project implements a comprehensive real-time data pipeline that ingests streaming trip events, processes them using AWS Lambda, stores data in DynamoDB, and computes daily KPIs into Amazon S3 using scheduled aggregation jobs orchestrated via Amazon EventBridge.

**Key Features:**
- Real-time event processing with automatic scaling
- Data validation and deduplication
- Automated daily analytics computation
- Serverless architecture with built-in observability
- CI/CD pipeline for continuous deployment

## Getting Started

### Prerequisites

- AWS CLI configured with appropriate credentials
- Python 3.8 or higher
- Git

### Clone the Repository

```bash
git clone https://github.com/Eugenia-DE/nsp-bolt_ride_real-time_trip_processing.git
cd nsp-bolt_ride_real-time_trip_processing
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

## Architecture

### AWS Services Used

| Service | Purpose |
|---------|---------|
| **Kinesis Data Streams** | Ingests real-time trip event data via `trip_events_stream` |
| **AWS Lambda** | Processes events and aggregates KPIs |
| **DynamoDB** | Stores trip records with `trip_id` as primary key |
| **Amazon S3** | Stores aggregated daily KPIs in JSON format |
| **EventBridge** | Triggers scheduled KPI aggregation jobs |
| **SNS** | Sends notifications for Lambda processing errors |
| **CloudWatch Logs** | Provides observability and monitoring |
| **GitHub Actions** | Handles CI/CD pipeline for testing and deployment |

### Data Flow

```
CSV Data → simulate_stream.py → Kinesis → Lambda → DynamoDB → EventBridge → Lambda → S3
```

## Project Structure

```
nsp-bolt-ride/
├── data/                          # Source CSV files (trip_start.csv, trip_end.csv)
├── schemas/                       # JSON schema for event validation
├── lambda/
│   ├── process_trip_event/        # Lambda function: Kinesis to DynamoDB
│   └── aggregate_trip_kpis/       # Lambda function: Aggregate KPIs to S3
├── src/ingestion/
│   └── simulate_stream.py         # Python simulator for real-time streaming
├── tests/                         # Unit tests for ingestion logic
├── requirements.txt               # Python dependencies
└── .github/workflows/             # CI/CD GitHub Actions workflows
```

## Implementation Details

### Data Validation and Processing

**Schema Validation:**
- All events must conform to the JSON schema defined in `schemas/trip_event_schema.json`
- Schema includes required fields: `trip_id`, `event_type`, `fare_amount`, timestamps

**Deduplication Strategy:**
- Simulator generates unique hash based on `trip_id` + `event_type`
- Prevents duplicate event processing

**Event Processing:**
- Events are randomized across `trip_start` and `trip_end` types
- Real-time validation using `jsonschema` library

### Database Schema

**DynamoDB Table: `trips`**
- Primary Key: `trip_id` (String)
- Supports upsert operations for incremental updates
- Automatically adds `status: "completed"` when both pickup and dropoff times exist

### KPI Metrics

The pipeline computes the following daily metrics for completed trips:

- **total_fare**: Sum of all fare amounts
- **count_trips**: Total number of completed trips
- **average_fare**: Mean fare amount
- **max_fare**: Highest fare of the day
- **min_fare**: Lowest fare of the day

**Output Location:** `s3://trip-analytics-kpi-bucket/kpis/YYYY-MM-DD.json`

### KPI Computation Logic

Below is an example of the aggregated daily KPI output written to S3:

![alt text](<images/2025-07-11 225257.png>)

![alt text](<images/2025-07-11 225007.png>)

## Deployment and Usage

### Step 1: AWS Resource Setup

Create the following AWS resources:

1. **Kinesis Data Stream**: `trip_events_stream`
2. **DynamoDB Table**: `trips` with `trip_id` as primary key
3. **S3 Bucket**: `trip-analytics-kpi-bucket` with `kpis/` prefix
4. **IAM Roles**: Appropriate permissions for Lambda functions

### Step 2: Lambda Function Deployment

Deploy both Lambda functions:

1. **process_trip_event**: Triggered by Kinesis stream
2. **aggregate_trip_kpis**: Scheduled via EventBridge

### Step 3: Data Simulation

Execute the data streaming simulation:

```bash
python src/ingestion/simulate_stream.py
```

This will stream approximately 5,000 records to Kinesis with validation and deduplication.

### Step 4: Schedule Configuration

Configure EventBridge rule:
- Schedule: `rate(1 hour)` or `rate(1 day)`
- Target: `aggregate_trip_kpis` Lambda function
- Ensure proper IAM permissions for DynamoDB read and S3 write operations

## CI/CD Pipeline

### GitHub Actions Workflows

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `ci-dev.yml` | Push to `dev` branch | Runs unit tests and validation |
| `deploy-main.yml` | Push to `main` branch | Deploys Lambda functions to AWS |

### Testing

Run unit tests locally:

```bash
pytest tests/
```

## Monitoring and Troubleshooting

### Common Issues and Solutions

| Issue | Cause | Resolution |
|-------|-------|------------|
| `Float types not supported` | DynamoDB type conversion | Use `Decimal(str(value))` from `boto3.dynamodb.types` |
| `ResourceNotFoundException` | Incorrect stream name/region | Verify resource names and AWS region configuration |
| KPI aggregation not updating | EventBridge misconfiguration | Confirm rule region and Lambda target configuration |
| Duplicate records | Simulator rerun | Clear `seen_event_ids` or implement Lambda-level deduplication |

### Monitoring

- **CloudWatch Logs**: Monitor Lambda execution logs
- **CloudWatch Metrics**: Track Kinesis throughput and Lambda performance
- **SNS Notifications**: Receive alerts for processing errors

## Business Value

This pipeline enables:

- **Real-time Analytics**: Near-instantaneous processing of ride activity
- **Scalable Architecture**: Automatically handles varying data volumes
- **Operational Insights**: Daily metrics for business intelligence
- **Foundation for Growth**: Extensible architecture for additional analytics features

## Technical Considerations

- EventBridge configuration managed manually (not Infrastructure as Code)
- Repository uses `dev` and `main` branches for development workflow
- Schema validation ensures data quality at ingestion
- Serverless design minimizes operational overhead

## Contributing

1. Fork the repository
2. Create a feature branch from `dev`
3. Implement changes with appropriate tests
4. Submit a pull request

## License

This project is part of training requirements.

## Support

For questions or issues, please open a GitHub issue in the repository.