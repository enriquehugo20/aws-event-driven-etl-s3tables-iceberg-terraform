# Serverless Event-Driven ETL Pipeline (AWS S3 Tables & Iceberg)

An enterprise-grade, production-ready data engineering pipeline designed to showcase advanced proficiency in modern AWS cloud data architectures, infrastructure-as-code, and automated CI/CD workflows.

---

## 🛠️ Tech Stack & Architecture Highlights

* **Infrastructure as Code (IaC):** Fully provisioned using **Terraform**, ensuring reproducible, version-controlled cloud environments and strict separation of concerns between code (`src/`) and infrastructure (`terraform/`).
* **CI/CD Automation:** Automated deployment pipeline built with **GitHub Actions**, securely authenticating via GitHub Secrets to apply infrastructure changes and sync Glue scripts on every push.
* **Modern Event-Driven Architecture:** Leverages **Amazon EventBridge** natively integrated with **Amazon S3** to capture object creation events, directly triggering downstream processing without expensive or complex intermediary compute layers (like Lambda).
* **Modern Table Formats (Apache Iceberg):** Utilizes native **S3 Table Buckets**, eliminating the traditional operational overhead of maintaining AWS Glue Crawlers or manual Athena partition projections.
* **Scalable Data Processing:** Implements **AWS Glue (PySpark 4.0)** for distributed data cleansing, schema standardization, and row enrichment (`processed_datetime`, `etl_status`).
* **Observability & Reliability:** Configured with **Amazon CloudWatch** alarms tracking job execution failures, integrated with **Amazon SNS** for real-time error notifications.
* **Audit & Compliance:** Dedicated state logging bucket capturing execution metadata and JSON status reports for every ETL run, ensuring end-to-end traceability.

---

## 🏗️ Architecture & Modern Data Stack

* **Automated Event-Driven Ingestion**: 
  * Leverages **Amazon EventBridge** natively integrated with **Amazon S3** (`aws_s3_bucket_notification`) to capture `Object Created` events.
  * **Why EventBridge over traditional SNS/SQS/Lambda chains?** Eliminates the need for intermediary Lambda functions, drastically reducing operational overhead, latency, and IAM complexity by allowing EventBridge to invoke AWS Glue targets directly.
* **Modern Table Formats (S3 Tables & Apache Iceberg)**: 
  * Utilizes native **S3 Table Buckets** (`aws_s3tables_table_bucket`) configured for **Apache Iceberg**.
  * **Why S3 Tables?** Completely removes the traditional overhead of maintaining AWS Glue Crawlers or configuring Athena Partition Projection, providing high-performance table formats with built-in maintenance operations and open-standards compatibility.
* **Serverless Processing Engine**: 
  * **AWS Glue (PySpark)** running on Glue version 4.0 (`G.1X` workers) to execute data cleansing, standardization, column enrichment (`processed_datetime`, `etl_status`), and Parquet/Iceberg transformations.
* **Auditability & Observability**: 
  * Dedicated `state` bucket tracking pipeline execution metadata and JSON status logs for every run.
  * **Amazon CloudWatch** alarms tracking Glue job failures, coupled with **Amazon SNS** for real-time email alerting.
* **Infrastructure as Code & CI/CD**: 
  * Fully declarative infrastructure managed via **Terraform**.
  * Automated deployment pipeline orchestrated through **GitHub Actions** using secure OIDC/Secret authentication.

---

## 🚀 Key Engineering Highlights (AWS DEA Alignment)

* **Decoupled Architecture**: Strict separation of source code (`src/`) and infrastructure code (`terraform/`).
* **Stateless Deployment**: Automated packaging and syncing of PySpark scripts to S3 via Terraform resources (`aws_s3_object`).
* **Zero-Crawler Analytics**: Direct integration with query engines via Apache Iceberg specifications on S3 Tables.