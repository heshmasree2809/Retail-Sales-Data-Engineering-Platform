# Retail Sales Data Engineering Platform

An end-to-end AWS data engineering pipeline that automates retail sales ingestion, PySpark ETL processing, data quality validation, incremental data loading, and analytical reporting using AWS services.

## Overview

Retail Sales Data Engineering Platform simulates a daily ERP/POS data pipeline that processes retail transaction data and transforms raw CSV files into structured, analytics-ready datasets.

The platform uses event-driven ingestion, serverless orchestration, distributed data processing, and partitioned storage to support reliable data processing and business analytics.

## Architecture

```text
             ERP / POS DATA SOURCES
                       |
                       v
                 Amazon S3
                  (Raw Layer)
                       |
                       v
                  AWS Lambda
                 (Event Trigger)
                       |
                       v
                AWS Step Functions
                 (Orchestration)
                       |
                       v
                   AWS Glue
                  (PySpark ETL)
                       |
                +------+------+
                |             |
                v             v
           Valid Data    Rejected Data
                |             |
                v             v
           S3 Curated     S3 Rejected
           (Parquet)       (CSV)
                |
                v
          Glue Data Catalog
                |
                v
            Amazon Athena
             (SQL Analytics)
                |
                v
          Amazon Redshift
          (Data Warehouse)
                |
                v
          Business Analytics
```

## Tech Stack

| Category | Technologies |
|---|---|
| Programming | Python, SQL |
| Data Processing | PySpark, AWS Glue |
| Cloud Platform | AWS |
| Storage | Amazon S3 |
| Orchestration | AWS Step Functions |
| Serverless Computing | AWS Lambda |
| Data Catalog | AWS Glue Data Catalog |
| Query Engine | Amazon Athena |
| Data Warehouse | Amazon Redshift |
| Monitoring | Amazon CloudWatch |
| Notifications | Amazon SNS |
| Visualization | Amazon QuickSight |
| Version Control | Git, GitHub |

## Key Features

- Event-driven data ingestion using Amazon S3 and AWS Lambda.
- Automated ETL orchestration using AWS Step Functions.
- Distributed data processing using AWS Glue and PySpark.
- Data cleaning, transformation, deduplication, and validation.
- Separation of valid and rejected records.
- Partitioned Parquet storage for analytical workloads.
- Metadata management using AWS Glue Data Catalog.
- Serverless SQL analytics using Amazon Athena.
- Incremental file-based data processing.
- Failure handling and pipeline monitoring.
- Dimensional data modeling for warehouse analytics.

## Data Pipeline

### 1. Data Ingestion

Daily retail sales CSV files simulate ERP/POS transaction feeds.

Source datasets include:

- Sales transactions
- Product master data
- Store master data

New files are uploaded to the Amazon S3 raw layer.

### 2. Event-Driven Processing

Amazon S3 ObjectCreated events trigger an AWS Lambda function.

Lambda extracts the bucket name and object key and starts the corresponding AWS Step Functions execution.

Deterministic execution names help prevent duplicate workflow executions for the same input file.

### 3. Pipeline Orchestration

AWS Step Functions manages the ETL workflow.

```text
Validate ERP File
       |
       v
Run Glue ETL
       |
       v
Check Curated Crawler
       |
       v
Start Crawler
       |
       v
Wait for Completion
       |
       v
Validate Crawler Status
       |
       v
Pipeline Success
```

Failures are handled through the workflow's notification path using Amazon SNS.

### 4. PySpark ETL Processing

AWS Glue processes incoming sales data using PySpark.

Processing operations include:

- Reading sales, product, and store datasets.
- Schema standardization and data type conversion.
- Missing-value validation.
- Duplicate detection.
- Product and store data enrichment.
- Revenue and profit calculations.
- Ingestion metadata generation.
- Valid and rejected record separation.

### 5. Data Quality Validation

The pipeline implements business-level validation rules.

| Validation | Rule |
|---|---|
| Transaction ID | Required |
| Transaction Date | Valid date |
| Store ID | Required and valid |
| Product ID | Required and valid |
| Quantity | Greater than zero |
| Unit Price | Greater than zero |
| Payment Method | UPI, CARD, or CASH |
| Product Reference | Must exist in product master |
| Store Reference | Must exist in store master |
| Duplicate Records | Duplicate transaction IDs identified |

Invalid records are separated from valid records for further investigation.

### 6. Curated Data Storage

Validated records are stored in Amazon S3 using Parquet format.

```text
curated/
    sales/
        transaction_date=2026-09-25/
        transaction_date=2026-09-26/
        transaction_date=2026-09-27/
```

Rejected records are stored separately.

```text
rejected/
    sales/
        transaction_date=2026-09-25/
        transaction_date=2026-09-26/
```

Date-based partitioning supports efficient analytical queries.

### 7. Data Catalog and SQL Analytics

AWS Glue Data Catalog maintains metadata and partition information for curated datasets.

Amazon Athena enables SQL-based analysis without requiring dedicated database infrastructure.

Supported analytical use cases:

- Daily and monthly sales analysis.
- Product performance.
- Store and regional performance.
- Category-wise revenue.
- Payment method analysis.
- Customer purchase analysis.
- Revenue and profit analysis.
- Data quality validation.

## Incremental Data Processing

The pipeline uses incremental file-based ingestion.

Each newly uploaded sales file is processed independently.

```text
Day 1 -> sales_2026_09_25.csv
Day 2 -> sales_2026_09_26.csv
Day 3 -> sales_2026_09_27.csv
```

The ETL process writes individual date-based partitions instead of reprocessing the entire historical dataset.

Manual S3 uploads simulate incoming ERP/POS feeds, while event-driven processing automates the subsequent workflow.

## Data Warehouse Design

The planned Amazon Redshift warehouse follows a dimensional modeling approach.

### Warehouse Tables

```text
retail_dw/
    |
    |-- sales_stage_raw
    |-- sales_stage
    |-- product_stage
    |-- store_stage
    |
    |-- dim_product
    |-- dim_store
    |-- dim_date
    |
    |-- fact_sales
    |-- etl_file_control
```

### Star Schema

```text
             dim_date
                 |
                 |
dim_product -- fact_sales -- dim_store
                 |
                 |
             customer_id
```

The warehouse design supports analytical queries through fact and dimension tables.

The intended loading process uses staging tables and transaction IDs as business keys to support incremental warehouse updates.

## Monitoring and Failure Handling

| AWS Service | Responsibility |
|---|---|
| CloudWatch | Logs and operational monitoring |
| Step Functions | Workflow execution tracking |
| AWS Glue | ETL job monitoring |
| Amazon Athena | Query execution history |
| Amazon SNS | Failure notifications |

## Project Structure

```text
Retail-Sales-Data-Engineering-Platform/
│
├── lambda/
│   └── retail_s3_ingestion_trigger.py
│
├── step-functions/
│   └── retail_daily_sales_pipeline.json
│
├── glue/
│   └── retail_sales_pyspark_etl.py
│
├── athena/
│
├── redshift/
│
├── config/
│
├── README.md
│
└── .gitignore
```

## Execution Workflow

1. Upload a daily sales CSV file to the Amazon S3 raw directory.
2. S3 triggers the Lambda ingestion function.
3. Lambda starts the Step Functions workflow.
4. Step Functions invokes the AWS Glue ETL job.
5. Glue processes and validates transaction data.
6. Valid records are written as partitioned Parquet files.
7. Invalid records are stored separately.
8. Glue Crawler updates the Data Catalog.
9. Athena executes analytical SQL queries.
10. Verify pipeline execution and data quality through AWS monitoring services.

## Security and Configuration

- AWS credentials and database passwords are excluded from source control.
- Deployment-specific values are maintained using configuration placeholders.
- IAM roles provide service-level permissions.
- Sensitive configuration should be managed through secure AWS services such as Secrets Manager and Lambda environment variables.

## Project Status

**Implemented and tested:**
- Amazon S3 data ingestion.
- AWS Lambda event triggering.
- Step Functions orchestration.
- AWS Glue PySpark ETL.
- Data quality validation.
- Valid and rejected record separation.
- Partitioned Parquet storage.
- Glue Data Catalog.
- Amazon Athena analytics.

**Additional implementation and validation:**
- Amazon Redshift warehouse loading.
- Staging and incremental MERGE operations.
- Warehouse-level validation.
- QuickSight dashboard integration.

## Engineering Concepts Demonstrated

- Event-driven architecture
- ETL pipeline development
- Distributed data processing
- PySpark transformations
- Data quality engineering
- Incremental data ingestion
- Data lake architecture
- Parquet partitioning
- SQL analytics
- Dimensional data modeling
- Cloud orchestration
- Pipeline monitoring and failure handling

---
**Built with Python, PySpark, SQL, and AWS.**
