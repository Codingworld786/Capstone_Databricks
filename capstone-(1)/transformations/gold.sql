CREATE MATERIALIZED VIEW `capstone`.`gold`.`customer_360`
  (

    customer_id STRING, first_name STRING, last_name STRING, gender STRING, city STRING, state
    STRING, risk_category STRING, credit_score INT, total_accounts BIGINT, total_balance DOUBLE,
    total_loans BIGINT, total_outstanding_loan DOUBLE, total_transactions BIGINT,
    failed_transactions BIGINT, avg_loan_interest DOUBLE, avg_transaction_amount DOUBLE,
    last_transaction_date DATE, created_at TIMESTAMP
  ) AS
SELECT
  c.customer_id,
  c.first_name,
  c.last_name,
  c.gender,
  c.city,
  c.state,
  c.risk_category,
  c.credit_score,
  COUNT(DISTINCT a.account_id) AS total_accounts,
  SUM(a.current_balance) AS total_balance,
  COUNT(DISTINCT l.loan_id) AS total_loans,
  SUM(l.outstanding_amount) AS total_outstanding_loan,
  COUNT(DISTINCT t.transaction_id) AS total_transactions,
  SUM(
    CASE
      WHEN t.status = 'Failed' THEN 1
      ELSE 0
    END
  ) AS failed_transactions,
  ROUND(AVG(l.interest_rate), 2) AS avg_loan_interest,
  ROUND(AVG(t.amount), 2) AS avg_transaction_amount,
  MAX(t.transaction_date) AS last_transaction_date,
  CURRENT_TIMESTAMP() AS created_at
FROM
  capstone.silver.customers_dim c
    LEFT JOIN capstone.silver.accounts_cleaned a
      ON c.customer_id = a.customer_id
    LEFT JOIN capstone.silver.loans_cleaned l
      ON c.customer_id = l.customer_id
    LEFT JOIN capstone.silver.transactions_cleaned t
      ON a.account_id = t.from_account_id
      OR a.account_id = t.to_account_id
WHERE
  c.is_current = TRUE
GROUP BY
  c.customer_id,
  c.first_name,
  c.last_name,
  c.gender,
  c.city,
  c.state,
  c.risk_category,
  c.credit_score;


CREATE MATERIALIZED VIEW capstone.gold.mv_transaction_kpis
COMMENT "Aggregated transaction KPIs for dashboard metrics"
AS
SELECT
    DATE_TRUNC('month', transaction_date) AS month,
    channel,
    COUNT(*) AS total_transactions,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount,
    PERCENTILE_APPROX(amount, 0.5) AS median_amount,
    COUNT_IF(status = 'Failed') AS failed_transactions,
    ROUND(100.0 * COUNT_IF(status = 'Failed') / COUNT(*), 2) AS failure_rate
FROM capstone.silver.transactions_cleaned
GROUP BY month, channel
ORDER BY month, channel;

--loan kpi
CREATE OR REFRESH MATERIALIZED VIEW capstone.gold.mv_loan_kpis
COMMENT "Aggregated loan KPIs for performance and risk analysis"
AS
SELECT
    DATE_TRUNC('month', disbursement_date) AS disbursement_month,
    loan_type,
    COUNT(*) AS total_loans,
    COUNT_IF(loan_status = 'Active') AS active_loans,
    COUNT_IF(loan_status = 'Closed') AS closed_loans,
    COUNT_IF(loan_status = 'Default') AS defaulted_loans,
    ROUND(100.0 * COUNT_IF(loan_status = 'Default') / COUNT(*), 2) AS default_rate,
    SUM(loan_amount) AS total_disbursed_amount,
    SUM(outstanding_amount) AS total_outstanding_amount,
    AVG(outstanding_amount) AS avg_outstanding_amount,
    AVG(interest_rate) AS avg_interest_rate
FROM capstone.silver.loans_cleaned
GROUP BY disbursement_month, loan_type
ORDER BY disbursement_month, loan_type;

-- Business KPIs
--1. Active customers/accounts by month 
CREATE OR REFRESH MATERIALIZED VIEW capstone.gold.mv_active_customers_accounts_monthly
COMMENT "Monthly active customers and accounts based on transaction activity"
AS
SELECT
    DATE_TRUNC('month', t.transaction_date) AS activity_month,
    COUNT(DISTINCT t.from_account_id) AS active_accounts,
    COUNT(DISTINCT a.customer_id) AS active_customers
FROM capstone.silver.transactions_cleaned t
LEFT JOIN capstone.silver.accounts_cleaned a
    ON t.from_account_id = a.account_id
GROUP BY activity_month
ORDER BY activity_month;

--2. Monthly transaction volume and failure rate 
CREATE OR REFRESH MATERIALIZED VIEW capstone.gold.mv_monthly_transaction_kpis
COMMENT "Monthly transaction volume and failure rate"
AS
SELECT
    DATE_TRUNC('month', transaction_date) AS month,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN LOWER(status) = 'failed' THEN 1 ELSE 0 END) AS failed_transactions,
    ROUND(
        (SUM(CASE WHEN LOWER(status) = 'failed' THEN 1 ELSE 0 END) * 100.0) / COUNT(*),
        2
    ) AS failure_rate_percent
FROM capstone.silver.transactions_cleaned
GROUP BY month
ORDER BY month;

--3. Wealth segment assignment 
CREATE OR REFRESH MATERIALIZED VIEW capstone.gold.mv_wealth_segments
AS
SELECT
  customer_id,
  first_name,
  last_name,
  annual_income,
  CASE
    WHEN annual_income < 1000000 THEN 'Mass'
    WHEN annual_income BETWEEN 1000000 AND 10000000 THEN 'Affluent'
    WHEN annual_income BETWEEN 10000000 AND 50000000 THEN 'HNI'
    ELSE 'Ultra HNI'
  END AS wealth_segment,
  current_timestamp() AS created_at
FROM capstone.silver.customers_cleaned;

--4. Credit score distributions, risk evaluation 

CREATE OR REFRESH MATERIALIZED VIEW capstone.gold.mv_credit_risk_analysis
AS
SELECT
  customer_id,
  first_name,
  last_name,
  credit_score,
  CASE
    WHEN credit_score >= 750 THEN 'Low Risk'
    WHEN credit_score BETWEEN 650 AND 749 THEN 'Medium Risk'
    ELSE 'High Risk'
  END AS risk_category,
  COUNT(*) OVER (PARTITION BY
    CASE
      WHEN credit_score >= 750 THEN 'Low Risk'
      WHEN credit_score BETWEEN 650 AND 749 THEN 'Medium Risk'
      ELSE 'High Risk'
    END
  ) AS customer_count_in_category
FROM capstone.silver.customers_cleaned;

-- 5. Compliance metrics
CREATE OR REFRESH MATERIALIZED VIEW capstone.gold.mv_compliance_metrics AS
SELECT
  COUNT(*) AS total_customers,
  SUM(CASE WHEN kyc_status = 'Completed' THEN 1 ELSE 0 END) AS kyc_completed,
  SUM(CASE WHEN kyc_status <> 'Completed' OR kyc_status IS NULL THEN 1 ELSE 0 END) AS kyc_pending,
  ROUND(SUM(CASE WHEN kyc_status = 'Completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS kyc_completion_rate
FROM capstone.silver.customers_cleaned;

---6. Operational metrics
CREATE OR REFRESH MATERIALIZED VIEW capstone.gold.mv_operational_metrics AS
SELECT
  branch_code,
  ROUND(AVG(DATEDIFF(disbursement_date, approval_date)), 2) AS avg_loan_processing_days,
  ROUND(SUM(CASE WHEN overdue_amount > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS overdue_loan_rate
FROM capstone.silver.loans_cleaned
GROUP BY branch_code;

-- BI Dashboards
CREATE OR REFRESH MATERIALIZED VIEW capstone.gold.mv_dashboard_summary AS
SELECT
  current_date() AS snapshot_date,
  
  -- Customers
  COUNT(DISTINCT c.customer_id) AS total_customers,
  SUM(CASE WHEN c.kyc_status = 'Completed' THEN 1 ELSE 0 END) AS kyc_completed,
  
  -- Accounts
  COUNT(DISTINCT a.account_id) AS total_accounts,
  ROUND(AVG(a.current_balance), 2) AS avg_account_balance,
  
  -- Loans
  COUNT(DISTINCT l.loan_id) AS total_loans,
  ROUND(AVG(l.interest_rate), 2) AS avg_loan_interest,
  ROUND(SUM(l.outstanding_amount), 2) AS total_outstanding_loans,
  
  -- Transactions
  COUNT(DISTINCT t.transaction_id) AS total_transactions,
  SUM(CASE WHEN t.status = 'Failed' THEN 1 ELSE 0 END) AS failed_transactions,
  ROUND(SUM(CASE WHEN t.status = 'Failed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS failure_rate
FROM capstone.silver.customers_cleaned c
LEFT JOIN capstone.silver.accounts_cleaned a ON c.customer_id = a.customer_id
LEFT JOIN capstone.silver.loans_cleaned l ON c.customer_id = l.customer_id
LEFT JOIN capstone.silver.transactions_cleaned t ON a.account_id = t.from_account_id
GROUP BY current_date();



