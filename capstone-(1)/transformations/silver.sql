-- data ingestion from bronze to silver
-- by adding expectations

CREATE STREAMING TABLE capstone.silver.customers_cleaned
(
  CONSTRAINT valid_customerid EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_email EXPECT (email IS NOT NULL AND email LIKE '%@%') ON VIOLATION DROP ROW,
  CONSTRAINT valid_dob EXPECT (date_of_birth IS NOT NULL AND date_of_birth <= current_date()) ON VIOLATION DROP ROW,
  CONSTRAINT valid_annual_income EXPECT (annual_income >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_credit_score EXPECT (credit_score BETWEEN 300 AND 900) ON VIOLATION DROP ROW,
  CONSTRAINT valid_kyc EXPECT (kyc_status IN ('Verified', 'Pending')) ON VIOLATION DROP ROW
)
AS
SELECT DISTINCT
    c.*
FROM STREAM capstone.bronze.customers AS c;



create streaming table capstone.silver.transactions_cleaned
(constraint valid_transactionid expect (transaction_id is not null) on violation drop row,
constraint valid_fee_amount expect (fee_amount >= 0) on violation drop row,
constraint valid_amount expect (amount >= 0) on violation drop row,
constraint valid_from_account EXPECT (from_account_id IS NOT NULL) ON VIOLATION DROP ROW,
CONSTRAINT valid_to_account EXPECT ( -- to_account_id is not null for all transactions except Transfer and Bill Payment
  NOT (
    transaction_type IN ('Transfer', 'Bill Payment') 
    AND to_account_id IS NULL
  )
) ON VIOLATION DROP ROW,
-- transaction date is not null and is not in the future
CONSTRAINT valid_transaction_date EXPECT (
  transaction_date IS NOT NULL 
  AND transaction_date <= current_date()
) ON VIOLATION DROP ROW

)
as
select distinct * from stream capstone.bronze.transactions;




create streaming table capstone.silver.accounts_cleaned
(constraint valid_accountid expect (account_id is not null) on violation drop row,
constraint valid_available_balance expect (available_balance >= 0) on violation drop row,
constraint valid_current_balance expect (current_balance >= 0) on violation drop row,
CONSTRAINT valid_customer_ref EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
CONSTRAINT valid_opening_date EXPECT (
  opening_date IS NOT NULL AND opening_date <= current_date()
) ON VIOLATION DROP ROW

)
as
select distinct
    a.*,
    a.nominee_details.nominee_name AS nominee_name,
    a.nominee_details.relationship AS nominee_relationship 
from stream capstone.bronze.accounts as a;


CREATE STREAMING TABLE capstone.silver.branches_cleaned
(
  CONSTRAINT valid_branch_code EXPECT (branch_code IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_ifsc EXPECT (ifsc_code IS NOT NULL AND length(ifsc_code) BETWEEN 8 AND 15) ON VIOLATION DROP ROW,
  CONSTRAINT valid_compliance EXPECT (compliance_score BETWEEN 0 AND 100) ON VIOLATION DROP ROW,
  CONSTRAINT valid_establishment_date EXPECT (establishment_date <= current_date()) ON VIOLATION DROP ROW,
  CONSTRAINT valid_is_active EXPECT (is_active IN (true, false)) ON VIOLATION DROP ROW
)
AS
SELECT DISTINCT
    b.*,
    b.address.city AS branch_city,
    b.address.state AS branch_state,
    b.address.pincode AS branch_pincode,
    b.operational_details.opening_time AS opening_time,
    b.operational_details.closing_time AS closing_time,
    b.operational_details.working_days AS working_days,
    b.staff_details.branch_manager AS branch_manager,
    b.staff_details.total_employees AS total_employees
FROM STREAM capstone.bronze.branches AS b;




-- loan


CREATE STREAMING TABLE capstone.silver.loans_cleaned
(
  CONSTRAINT valid_loan_id EXPECT (loan_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_account_id EXPECT (account_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_loan_amount EXPECT (loan_amount > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_interest_rate EXPECT (interest_rate >= 0 AND interest_rate <= 100) ON VIOLATION DROP ROW,
  CONSTRAINT valid_approval_date EXPECT (approval_date <= current_date()) ON VIOLATION DROP ROW,
  CONSTRAINT valid_disbursement_date EXPECT (disbursement_date >= approval_date) ON VIOLATION DROP ROW,
  CONSTRAINT valid_tenure EXPECT (tenure_months > 0) ON VIOLATION DROP ROW
)
AS
SELECT DISTINCT
    l.*,

    -- Collateral fields
    l.collateral_details.collateral_type AS collateral_type,
    l.collateral_details.collateral_value AS collateral_value,
    l.collateral_details.collateral_address.city AS collateral_city,
    l.collateral_details.collateral_address.state AS collateral_state,
    l.collateral_details.collateral_address.pincode AS collateral_pincode,
    l.collateral_details.collateral_address.street AS collateral_street,

    -- Guarantor fields
    l.guarantor_details.guarantor_name AS guarantor_name,
    l.guarantor_details.relationship AS guarantor_relationship,
    l.guarantor_details.guarantor_pan AS guarantor_pan,
    l.guarantor_details.guarantor_phone AS guarantor_phone

FROM STREAM capstone.bronze.loans AS l;


--This `MERGE` tracks customer history using SCD Type 2.
--It closes old records when key details change and inserts new active versions.
--Only one version per customer stays marked as current.











