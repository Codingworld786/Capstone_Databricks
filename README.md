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

<img width="377" height="247" alt="image" src="https://github.com/user-attachments/assets/1d40ed72-e2fb-47f4-9a3f-af4d7f2122bc" />

Step 2: Data Cleaning & Standardization (Silver Layer)

Using SQL transformations, I cleaned null values, standardized date formats, and removed duplicates.
At this stage, datasets were joined and renamed consistently for better readability.

<img width="426" height="340" alt="image" src="https://github.com/user-attachments/assets/e4d9e7e9-0a75-41d9-864c-b221c18c86f9" />

Step 3: Gold Layer – Business-Focused Tables

The gold layer holds curated and aggregated datasets optimized for dashboards and Genie.
Here’s what each view does:

1. customer_m360

A unified customer-level profile that brings together loan, transaction, and credit data.
It helps in understanding overall customer behavior and engagement across products.
Includes:

Customer demographics and region

Active/inactive status

Loan and transaction summaries

Lifetime value indicators

[screenshot here of customer_m360 schema]

2. credit_risk_analysis

Analyzes customer credit scores and assigns them to risk segments.
Logic:

Credit score ≥ 750 → Low Risk

650–749 → Medium Risk

< 650 → High Risk

Also calculates total customers in each segment for quick KPI breakdowns.
Useful for portfolio monitoring and compliance teams.

[screenshot here of credit score segmentation chart]

3. transactions_kpi

Tracks monthly and channel-level transaction metrics.
Includes:

Total volume by month

Failure rates

Average and median transaction values

Channel mix (e.g., online, branch, mobile)

This view feeds the executive dashboard and helps spot operational trends.

<img width="506" height="449" alt="image" src="https://github.com/user-attachments/assets/180deaf3-59d5-4305-90fb-d79ab47e6924" />

4. dashboard_summary

A consolidated view that merges KPIs from other gold tables.
It acts as the single source of truth for dashboards and Genie queries.
Includes:

Active customers by month

Total loan amounts and defaults

Credit risk distribution

Transaction health (success/failure trends)

Customer Risk Breakdown
<img width="1188" height="737" alt="image" src="https://github.com/user-attachments/assets/11be8282-67c3-452c-95fb-706d85fe01bf" />
Monthly Transaction Volumne:
<img width="1574" height="881" alt="image" src="https://github.com/user-attachments/assets/d9a331d6-be9e-4d41-ab8b-c7844afc647b" />

Step 5: Genie Q&A Integration

The gold views were added to Databricks Genie, which lets you query data using natural language.
Example queries I tested:

What is the total amount disbursed in loans each month?
<img width="1532" height="812" alt="image" src="https://github.com/user-attachments/assets/5faed228-3461-445e-b8bd-8ca634d1c592" />



What are the total closed loans by month?
<img width="1455" height="715" alt="image" src="https://github.com/user-attachments/assets/04af92d3-dd24-4f12-aeb8-33291e6579d6" />


What are the total closed loans for each loan type?
<img width="1419" height="713" alt="image" src="https://github.com/user-attachments/assets/78928064-4080-445e-9760-76aea8c933d1" />

Step 6: Validation & Final Checks

To confirm everything worked as expected:

Verified row counts between layers

Checked aggregation logic in gold tables

Validated KPIs matched dashboard visuals

<img width="851" height="337" alt="image" src="https://github.com/user-attachments/assets/79746be7-f1b8-411f-a868-dd2a20cd28b9" />
<img width="856" height="326" alt="image" src="https://github.com/user-attachments/assets/a4be3d30-3edf-41e5-8406-8044a0e29e0c" />

Results Summary

✅ Built gold-layer tables for business analytics
✅ Created interactive Databricks dashboards
✅ Enabled natural language querying with Genie
✅ Automated KPI-ready pipeline from raw data

Future Improvements

Add streaming ingestion for near real-time KPIs

Automate Genie dataset refresh

Integrate alerts for KPI threshold breaches





