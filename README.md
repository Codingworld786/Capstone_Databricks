Capstone Project – Financial Data Pipeline & Dashboard
Overview

This project walks through how I built a complete data pipeline from raw data ingestion to final dashboards and natural language querying using Databricks. Everything — from cleaning and transformations to gold-level analytics — was done from scratch.

The main goal was to process financial and customer datasets, generate useful KPIs (like transaction volumes, loan defaults, and risk segments), and visualize them through Databricks SQL dashboards and Genie Q&A.

capstone/
│
├── transformation/
│   ├── bronze_to_silver.sql
│   ├── silver_to_gold.sql
│   └── ...
│
├── exploration/
│   ├── kpi_queries.sql
│   ├── risk_analysis.sql
│   └── ...
│
└── README.md

Bronze Layer – raw ingestion
Silver Layer – cleaned and standardized data
Gold Layer – aggregated tables and KPI-ready materialized views

Step 1: Data Ingestion (Bronze Layer)

All raw datasets were first loaded into the Bronze layer. This included customer data, transaction logs, and loan details. The goal was to keep them in their original form for reproducibility and lineage tracking.

<img width="335" height="276" alt="image" src="https://github.com/user-attachments/assets/9bb7e469-84b0-4dbe-bdc6-f221a93e47b4" />

