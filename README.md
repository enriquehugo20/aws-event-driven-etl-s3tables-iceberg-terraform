# AWS Event-Driven ETL Pipeline with Apache Iceberg

## 1. Project Overview

This project implements an event-driven ETL pipeline on AWS to ingest, transform, and query transactional sales data using managed cloud services and Infrastructure as Code (IaC).

The pipeline processes CSV files uploaded to Amazon S3, triggers an AWS Glue workflow through Amazon EventBridge, transforms the data with PySpark, and stores the results in an Apache Iceberg table hosted in Amazon S3 Tables. The resulting dataset is queried using Amazon Athena, while Amazon CloudWatch and Amazon SNS provide monitoring and failure notifications.

Infrastructure deployment and teardown are automated through Terraform and GitHub Actions.

### Key Features

- Event-driven data ingestion and processing.
- Serverless ETL transformations with AWS Glue and PySpark.
- Apache Iceberg table storage through Amazon S3 Tables.
- SQL analytics using Amazon Athena.
- Infrastructure provisioning with Terraform.
- Continuous deployment triggered by GitHub Actions.
- Manual infrastructure teardown through a separate GitHub Actions workflow.
- Monitoring, logging, and failure notifications.
- Iceberg table maintenance configuration for compaction and snapshot management.
- Separate Terraform remote state storage, provisioned before the main infrastructure deployment.

## 2. Architecture Overview

### Data Flow

```text
CSV Source Files
       |
       v
Amazon S3 - Raw Data Bucket
       |
       v
Amazon EventBridge
       |
       v
AWS Glue Workflow
       |
       v
AWS Glue ETL Job (PySpark)
       |
       v
Amazon S3 Tables
(Apache Iceberg)
       |
       v
Amazon Athena
       |
       v
SQL Analytics and Reporting

Monitoring and Operations:
Amazon CloudWatch + Amazon SNS

Infrastructure Management:
Terraform + GitHub Actions
```

### Processing Workflow

1. **Data ingestion:** CSV files are uploaded to the raw Amazon S3 bucket.
2. **Event detection:** Amazon EventBridge detects the configured S3 event and initiates the processing workflow.
3. **Workflow orchestration:** AWS Glue Workflow coordinates the ETL execution.
4. **Data transformation:** AWS Glue runs a PySpark job to read, validate, and transform the source data.
5. **Data enrichment:** The job standardizes product categories and adds processing metadata.
6. **Data storage:** The transformed records are appended to an Apache Iceberg table in Amazon S3 Tables.
7. **Data analytics:** Amazon Athena queries the processed dataset using SQL.
8. **Monitoring:** Amazon CloudWatch collects execution logs and metrics, while Amazon SNS supports failure notifications.

## 3. Technology Stack

- **Cloud platform:** Amazon Web Services (AWS)
- **Object storage:** Amazon S3
- **Event-driven architecture:** Amazon EventBridge
- **ETL and orchestration:** AWS Glue
- **Data processing:** Python and PySpark
- **Table format:** Apache Iceberg
- **Managed table storage:** Amazon S3 Tables
- **Analytics:** Amazon Athena
- **Monitoring and logging:** Amazon CloudWatch
- **Notifications:** Amazon SNS
- **Infrastructure as Code:** Terraform
- **CI/CD automation:** GitHub Actions
- **Source control:** Git and GitHub
- **Query language:** SQL

## 4. Data Engineering and ETL Processing

### Source Data

The pipeline processes transactional sales data provided in CSV format.

Example:

```csv
transaction_id,customer_id,product_category,amount,store_location,transaction_date
TXN-1001,CUST-892,Electronics,299.99,New York,2026-06-01
TXN-1002,CUST-104,Clothing,45.50,Los Angeles,2026-06-01
TXN-1003,CUST-332,Home & Garden,120.00,Chicago,2026-06-02
TXN-1004,CUST-892,Electronics,850.00,New York,2026-06-03
TXN-1005,CUST-511,Clothing,22.99,Miami,2026-06-03
```

### Transformation Logic

The AWS Glue PySpark job performs the following operations:

- Reads transactional data from the raw S3 location.
- Processes the source records using Spark DataFrames.
- Standardizes product categories by converting them to uppercase.
- Adds a `processed_datetime` timestamp to identify when records were processed.
- Adds an `etl_status` field to track the processing result.
- Writes the transformed records to the target Iceberg table.
- Appends the processed records to the existing table.

The target table is referenced in the Glue job as:

```python
s3tables.sales.sales
```

The write operation uses the Iceberg-compatible Spark DataFrameWriterV2 API.

### Target Schema

The processed dataset contains eight columns:

| Column | Description |
|---|---|
| `transaction_id` | Unique transaction identifier |
| `customer_id` | Customer identifier |
| `product_category` | Standardized product category |
| `amount` | Transaction amount |
| `store_location` | Store or transaction location |
| `transaction_date` | Date associated with the transaction |
| `processed_datetime` | Timestamp indicating when processing occurred |
| `etl_status` | ETL processing status |

The Iceberg table schema is configured through Terraform to align with the data written by the Glue job.

## 5. Apache Iceberg and Table Maintenance

### Amazon S3 Tables

Amazon S3 Tables provides managed storage for the Apache Iceberg table. This allows the pipeline to maintain a structured analytical dataset on Amazon S3 and query it through supported AWS analytics integrations.

The table is organized under the `sales` namespace and is named `sales`.

### Maintenance Configuration

The table is configured with the following maintenance settings:

- **Iceberg compaction:** Enabled.
- **Target compaction file size:** 256 MB.
- **Snapshot management:** Enabled.
- **Maximum snapshot age:** 72 hours.
- **Minimum snapshots to retain:** 2.

The 256 MB compaction target is a configurable balance between file size and query efficiency for this workload. It is a target rather than a guarantee that every file will have exactly that size.

Snapshot management settings define the configured age and retention parameters. Actual maintenance behavior depends on the service's implementation and execution of the maintenance operations.

### Infrastructure Dependencies

The Terraform configuration explicitly models dependencies between the S3 Tables resources:

- The S3 Tables bucket must exist before its namespace is created.
- The namespace must exist before the table is created.
- The table resource includes an explicit `depends_on` reference to the namespace resource.

This dependency ordering helps prevent resource creation errors caused by attempting to create the table before its namespace is available.

## 6. Infrastructure as Code with Terraform

Terraform provisions and manages the AWS infrastructure required by the pipeline.

### Managed Infrastructure

The main Terraform configuration includes the resources required for the ETL architecture, including:

- Amazon S3 buckets for raw data, ETL state, and scripts/logs.
- An Amazon S3 Tables bucket.
- The `sales` S3 Tables namespace.
- The `sales` Apache Iceberg table.
- AWS Glue job and workflow resources.
- IAM roles and permissions required by the ETL components.
- Amazon EventBridge rules and associated targets.
- Amazon CloudWatch logging and monitoring resources.
- Amazon SNS resources for notifications.
- Supporting Athena configuration, where included in the Terraform configuration.

The exact set of resources managed by Terraform is determined by the configuration in the `terraform/` directory.

### S3 Bucket Responsibilities

The project distinguishes between buckets with different operational purposes:

- **Raw data bucket:** Stores incoming CSV files.
- **ETL state bucket:** Supports the state-related storage used by the ETL application, as configured.
- **Scripts and logs bucket:** Stores Glue scripts and supports operational artifacts, including the configured Athena query results location.
- **S3 Tables bucket:** Stores the managed Apache Iceberg table.
- **`tf-state` bucket:** Stores Terraform's remote infrastructure state and must be provisioned before the main deployment when configured as the S3 backend.

The ETL state bucket and Terraform remote state bucket serve different purposes and should not be treated as interchangeable.

### Terraform Remote State and Bootstrap

The `tf-state` bucket is a prerequisite for infrastructure deployment. It must be created separately before the main Terraform workflow runs because Terraform initializes its configured backend during `terraform init`.

The bootstrap process is therefore separate from the deployment of the main ETL infrastructure.

Key considerations:

- The `tf-state` bucket must exist before the deployment workflow is executed.
- The GitHub Actions AWS identity must have the permissions required to access the remote state.
- The backend configuration in Terraform must point to the correct state bucket and key.
- Any configured state-locking mechanism must also be accessible to the deployment identity.
- The remote state bucket is maintained separately from the resources managed by the main Terraform configuration.

**Destroy behavior:** The Terraform Destroy workflow does not delete `tf-state`. Because the bucket is provisioned separately and is not part of the main configuration's managed resources, the main infrastructure teardown does not remove this backend bucket. Keeping it separate also preserves the remote state storage needed to manage future deployments.

The backend bucket should remain protected from accidental deletion and should be managed through its own controlled lifecycle.

## 7. CI/CD with GitHub Actions

The project uses separate GitHub Actions workflows for infrastructure deployment and teardown.

### Automated Deployment

The deployment workflow is triggered by a push to the `main` branch.

Its steps are:

1. Check out the repository.
2. Configure AWS credentials using GitHub repository secrets.
3. Install the configured Terraform CLI.
4. Initialize the Terraform backend.
5. Apply the Terraform configuration with automatic approval.

The main deployment command is:

```bash
terraform init
terraform apply -auto-approve
```

Both commands run from the `terraform/` directory.

The workflow automates infrastructure deployment after changes are pushed to `main`. The current workflow does not include separate Terraform validation, a reviewable plan stage, or a manual approval gate before applying changes.

### Manual Infrastructure Teardown

The destroy workflow is triggered manually through GitHub Actions using `workflow_dispatch`.

Its steps are:

1. Check out the repository.
2. Install Terraform.
3. Configure AWS credentials.
4. Initialize the Terraform backend.
5. Destroy the resources managed by the main Terraform configuration.

The teardown command is:

```bash
terraform destroy -auto-approve
```

The command runs from the `terraform/` directory. Starting the workflow manually is required, but the Terraform destroy operation itself does not request an additional interactive confirmation because `-auto-approve` is enabled.

The separately provisioned `tf-state` bucket is not deleted by this workflow.

### CI/CD Security Considerations

The existing workflow demonstrates automated deployment and controlled, manually initiated teardown. Potential improvements include:

- Replacing long-lived AWS access keys with GitHub Actions OpenID Connect (OIDC) and an appropriately restricted IAM role.
- Adding `terraform fmt -check` and `terraform validate`.
- Generating and reviewing a Terraform plan before applying changes.
- Using protected GitHub environments for production deployments.
- Restricting permissions for the destroy workflow.
- Protecting the remote state bucket against accidental deletion.
- Separating development and production environments where appropriate.

These are recommended improvements rather than claims about functionality already implemented.

## 8. Monitoring, Logging, and Notifications

The architecture incorporates AWS monitoring and operational visibility through CloudWatch and SNS.

### Amazon CloudWatch

CloudWatch supports operational monitoring through:

- AWS Glue execution logs.
- Job-level execution information and metrics, where configured.
- Troubleshooting of ETL failures.
- Monitoring of the pipeline's operational behavior.

### Amazon SNS

SNS is used to support failure notifications from the configured monitoring and notification flow. This helps make pipeline failures visible without requiring continuous manual inspection of the Glue console.

The actual notification behavior depends on the configured SNS subscriptions, alarm conditions, and event integrations.

## 9. Querying Data with Amazon Athena

Amazon Athena provides SQL access to the processed Iceberg table.

### Querying Transaction Records

Example query:

```sql
SELECT
    transaction_id,
    customer_id,
    product_category,
    amount,
    store_location,
    transaction_date,
    processed_datetime,
    etl_status
FROM sales.sales;
```

### Aggregating Sales by Product Category

The following query summarizes transaction volume and revenue by product category:

```sql
SELECT
    product_category,
    COUNT(*) AS transaction_count,
    ROUND(SUM(amount), 2) AS total_sales
FROM sales.sales
GROUP BY product_category
ORDER BY total_sales DESC;
```

### Athena Query Results

Athena requires an S3 output location for query results. This project uses the following configured location:

```text
s3://scripts-logs-csv-parquet-etl-v6-<account-id>/athena-results/
```

Replace `<account-id>` with the AWS account ID for the deployment.

The configured Athena workgroup and query settings must use an accessible output location with the appropriate IAM permissions.

## 10. Validation Results

The pipeline was tested using five sample transaction records.

### Verified Outcomes

- The CSV input was processed by the Glue ETL job.
- Product categories were standardized to uppercase.
- The `processed_datetime` and `etl_status` fields were populated.
- Five transaction records were returned through Athena.
- All five returned records had `etl_status = 'SUCCESS'`.
- The target Iceberg schema included the eight expected columns.
- Aggregation queries returned the expected transaction counts and sales totals.

### Expected Aggregation Results

| Product Category | Transaction Count | Total Sales |
|---|---:|---:|
| ELECTRONICS | 2 | 1,149.99 |
| HOME & GARDEN | 1 | 120.00 |
| CLOTHING | 2 | 68.49 |
| **Total** | **5** | **1,338.48** |

These results validate the sample dataset and the transformation and query path. They do not, by themselves, establish performance under large-scale production workloads.

## 11. Key Engineering Challenges and Solutions

### Resource Creation Order

**Challenge:** Dependent S3 Tables resources can fail when created before their prerequisites are available.

**Solution:** Model the bucket, namespace, and table as separate Terraform resources and explicitly define the table's dependency on the namespace.

### Schema Compatibility

**Challenge:** The destination table schema must match the columns written by the ETL job.

**Solution:** Align the Iceberg table schema with the eight output columns, including processing metadata.

### ETL and Analytics Integration

**Challenge:** Successfully writing data is not sufficient unless the resulting dataset is accessible to the analytics layer.

**Solution:** Validate the transformed records with Amazon Athena and execute aggregation queries against the target table.

### Remote State Management

**Challenge:** Terraform needs its remote backend available before it can initialize and deploy the main infrastructure.

**Solution:** Provision `tf-state` separately and configure the deployment workflow to initialize the backend before applying changes.

### Operational Visibility

**Challenge:** Event-driven pipelines need visibility into job execution and failure conditions.

**Solution:** Integrate CloudWatch logging and monitoring with the configured SNS notification flow.

## 12. Repository Structure

```text
.
├── .github/
│   └── workflows/
│       ├── deploy.yml
│       └── destroy.yml
├── src/
│   └── glue/
│       └── transform_job.py
├── terraform/
│   └── main.tf
├── .gitignore
└── README.md
```

The workflow filenames above are illustrative; use the actual filenames in the repository.

- **`.github/workflows/`** — GitHub Actions automation for deployment and teardown.
- **`src/glue/transform_job.py`** — PySpark transformation logic for the ETL job.
- **`terraform/main.tf`** — Terraform definitions for the AWS infrastructure.
- **`.gitignore`** — Excludes files that should not be committed.
- **`README.md`** — Project documentation, architecture, and operational instructions.

## 13. Deployment Prerequisites

Before deploying the infrastructure, ensure that the following requirements are satisfied:

- An AWS account with the required service access.
- Terraform installed locally if deploying outside GitHub Actions.
- A GitHub repository containing the Terraform configuration and ETL script.
- GitHub repository secrets configured for the AWS credentials used by the current workflows.
- The `tf-state` remote backend bucket provisioned before running `terraform init`, if the backend is configured to use it.
- A valid Terraform backend configuration.
- IAM permissions for the deployment identity to access the backend and manage the required infrastructure.
- The required AWS service integrations and resource configurations.
- An accessible S3 output location for Athena query results.

The deployment identity and Glue execution role serve different purposes: the former provisions infrastructure, while the latter provides permissions required by the ETL job at runtime.

## 14. Future Improvements

Potential next steps include:

- Implementing automated data quality checks and schema validation.
- Adding unit tests for PySpark transformation logic.
- Introducing duplicate detection and idempotent processing.
- Adding input validation and more detailed error handling.
- Implementing CI checks and Terraform plan review.
- Adopting OIDC for short-lived AWS credentials in GitHub Actions.
- Enhancing cost monitoring and resource usage analysis.
- Adding partitioning and query optimization based on observed workloads.
- Introducing separate deployment environments and promotion controls.
- Expanding pipeline testing to larger datasets and failure scenarios.

## 15. Skills Demonstrated

- AWS cloud architecture and service integration.
- Event-driven data engineering.
- ETL development with Python and PySpark.
- Amazon S3 storage organization.
- Apache Iceberg table management.
- Amazon S3 Tables integration.
- SQL analytics with Amazon Athena.
- Infrastructure as Code using Terraform.
- CI/CD automation with GitHub Actions.
- IAM permissions and cloud resource dependencies.
- Monitoring, logging, and failure notifications.
- Data transformation, metadata enrichment, and analytical validation.
- Remote state management and infrastructure lifecycle separation.

## Conclusion

This project demonstrates an end-to-end AWS data engineering workflow that combines event-driven ingestion, distributed data transformation, Iceberg-based analytical storage, and SQL querying. Terraform and GitHub Actions provide repeatable infrastructure deployment, while the separately provisioned `tf-state` bucket preserves the remote infrastructure state across deployment and teardown operations.

The result is a modular foundation for further development toward a more robust, secure, and production-oriented data platform.
