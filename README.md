# Azure Data Engineering Retail Platform

## Project Overview

This project presents the design and implementation of an end-to-end Azure Data Engineering platform built to support retail analytics using the Medallion Architecture (Bronze, Silver, Gold).

The solution integrates Azure SQL Database and a REST API as data sources, Azure Data Factory for data ingestion and orchestration, Azure Data Lake Storage Gen2 for scalable storage, and Azure Databricks for distributed data processing using Apache Spark. Data from multiple sources, including structured database tables and API-driven JSON datasets, is ingested into the Bronze layer, cleansed and standardized in the Silver layer, and transformed into business-ready analytical datasets in the Gold layer using Delta Lake.

The final outputs are visualized through an interactive Power BI dashboard, enabling insights into sales performance, product trends, store metrics, and transaction behavior. This project demonstrates practical experience in building modern cloud-based data pipelines, multi-source data integration, scalable ETL workflows, and analytics-driven data solutions.

---

##  Architecture

The solution follows a modern Data Lakehouse pattern:

**Azure SQL Database → Azure Data Factory → ADLS Gen2 (Bronze Layer) → Azure Databricks → Silver Layer → Gold Layer → Power BI**

---

## Technology Stack

- **Azure SQL Database** – Source system
- **REST API** – External data source
- **Azure Data Factory (ADF)** – Data ingestion & orchestration
- **Azure Data Lake Storage Gen2 (ADLS)** – Data lake storage
- **Azure Databricks** – Data transformation & processing
- **Delta Lake** – Optimized storage format
- **Power BI** – Business intelligence & reporting

## Author

**Chaitanya Karthik**  
Data Engineer | Azure | Databricks | Data Analytics

