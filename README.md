## Cloud-Native Data Analytics Platform (AWS + Power BI)

This project was developed as part of my MSc Computer Science dissertation in the UK.

### Problem
Business users struggle to interpret raw datasets and dashboards without technical expertise.

### Solution
A cloud-native analytics pipeline using AWS Lambda and S3 that automatically profiles datasets, validates data quality, and generates structured insights for Power BI dashboards.

### Architecture
- AWS S3 – data ingestion
- AWS Lambda – serverless processing
- SQL-based transformations
- Power BI – analytics & visualisation

### Key Features
- Automated ETL pipelines
- Data profiling and anomaly detection
- Structured outputs (CSV / JSON)
- Power BI integration
- Serverless, cost-efficient design

### Tech Stack
AWS, Lambda, S3, SQL, Node.js, Power BI

### Improvements for Production
- Orchestration using Step Functions
- Streaming ingestion
- Metadata-driven transformations
