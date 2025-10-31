# Capstone Project – Financial Data Pipeline & Dashboard

![Databricks](https://img.shields.io/badge/Databricks-SQL-blue?logo=databricks) ![SQL](https://img.shields.io/badge/SQL-Transformations-green) ![Dashboard](https://img.shields.io/badge/Dashboard-Interactive-orange) ![Genie](https://img.shields.io/badge/Genie-NLQ-purple)

---
## Project Structure

<img width="684" height="381" alt="image" src="https://github.com/user-attachments/assets/8570bf92-5c55-42d4-8e3a-4c23be493b41" />


## Overview

This project demonstrates a **complete end-to-end data pipeline** built from scratch using **Databricks**, transforming raw financial and customer data into **actionable KPIs**, **interactive dashboards**, and **natural language querying** via **Databricks Genie**.

From **raw ingestion (Bronze)** → **cleaned & standardized (Silver)** → **aggregated & KPI-ready (Gold)** — all transformations are modular, reproducible, and optimized for analytics.

---
## Data Layers  
**Bronze:** Raw ingestion, original schema preserved  
**Silver:** Cleaned, standardized, deduplicated  
**Gold:** Aggregated, business-ready views & KPIs  


**Pipeline Steps**

**Step 1: Data Ingestion (Bronze Layer)**

Raw datasets loaded in original format for lineage and auditability:

- Customer profiles  
- Transaction logs  
- Loan applications & statuses...
  
<img src="https://github.com/user-attachments/assets/1d40ed72-e2fb-47f4-9a3f-af4d7f2122bc" alt="Bronze Ingestion">

**Step 2: Data Cleaning & Standardization (Silver Layer)**

SQL-based transformations:  
- Null handling  
- Date standardization  
- Deduplication  
- Consistent naming & joins  

<img src="https://github.com/user-attachments/assets/e4d9e7e9-0a75-41d9-864c-b221c18c86f9" alt="Silver Cleaning">

### Step 3: Gold Layer – Business-Focused Analytics

Curated views optimized for dashboards and Genie.

#### 1. `customer_m360`
Unified 360° customer view:  
- Demographics + region  
- Active/inactive status  
- Loan & transaction summaries  
- Lifetime value indicators  

#### 2. `credit_risk_analysis`
Analyzes customer credit scores and assigns them to risk segments.  
**Logic:**  
Credit score ≥ 750 → Low Risk
650–749 → Medium Risk
< 650 → High Risk

Includes:  
- Total customers per segment  
- Portfolio risk distribution  

<!-- <img src="https://github.com/user-attachments/assets/11be8282-67c3-452c-95fb-706d85fe01bf" alt="Credit Risk Chart"> -->

#### 3. `transactions_kpi`
Monthly & channel-level metrics:  
- Total volume  
- Success/failure rates  
- Avg/Median transaction value  
- Channel mix (online, branch, mobile)  

<!-- <img src="https://github.com/user-attachments/assets/d9a331d6-be9e-4d41-ab8b-c7844afc647b" alt="Transaction Volume"> -->

#### 4. `dashboard_summary`
Single source of truth for dashboards:  
- Active customers by month  
- Loan disbursements & defaults  
- Risk distribution  
- Transaction health trends  

Customer Risk Breakdown
<img width="1188" height="737" alt="image" src="https://github.com/user-attachments/assets/11be8282-67c3-452c-95fb-706d85fe01bf" />
Monthly Transaction Volumne:
<img width="1574" height="881" alt="image" src="https://github.com/user-attachments/assets/d9a331d6-be9e-4d41-ab8b-c7844afc647b" />

### Step 5: Genie Q&A Integration

Gold views exposed to **Databricks Genie** for natural language querying.

#### Example Queries:

**Q:** *What is the total amount disbursed in loans each month?*  
<img src="https://github.com/user-attachments/assets/5faed228-3461-445e-b8bd-8ca634d1c592" alt="Loan Disbursement">

**Q:** *What are the total closed loans by month?*  
<img src="https://github.com/user-attachments/assets/04af92d3-dd24-4f12-aeb8-33291e6579d6" alt="Closed Loans Monthly">

**Q:** *What are the total closed loans for each loan type?*  
<img src="https://github.com/user-attachments/assets/78928064-4080-445e-9760-76aea8c933d1" alt="Closed Loans by Type">

### Step 6: Validation & Final Checks

| Check                               | Status |
|-------------------------------------|--------|
| Row count consistency across layers | ✅     |
| Aggregation logic verification      | ✅     |
| Dashboard KPI alignment             | ✅     |

<img src="https://github.com/user-attachments/assets/79746be7-f1b8-411f-a868-dd2a20cd28b9" alt="Validation 1">  
<img src="https://github.com/user-attachments/assets/a4be3d30-3edf-41e5-8406-8044a0e29e0c" alt="Validation 2">

---

## Results Summary

| Achievement |
|-------------|
| ✅ Built gold-layer tables for business analytics |
| ✅ Created interactive Databricks SQL dashboards |
| ✅ Enabled natural language querying with Genie |
| ✅ Fully automated KPI pipeline from raw data |

---

## Future Improvements

- Add streaming ingestion for near real-time KPIs  
- Automate Genie dataset refresh  
- Integrate alerts for KPI threshold breaches (e.g., high default rates)

---

## Built With

- **Databricks SQL** – Transformations & Analytics  
- **Databricks Genie** – Natural Language Q&A  
- **Delta Lake** – Data reliability & versioning



