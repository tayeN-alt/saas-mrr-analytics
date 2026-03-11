SaaS MRR Analytics

A lightweight, self-contained data pipeline that transforms raw B2B SaaS subscription data into MRR Movements and Cohort Retention metrics.

For architecture decisions, data quality findings, business insights, and the Power BI dashboard guide — see documentation.docx.


Requirements

Python 3.8+
pandas

No other dependencies. SQLite is built into Python.

🚀 How to Run
bashpython run_model.py
The orchestration script performs the following:

Ingestion — Loads three raw CSVs into an in-memory SQLite database
Transformation — Executes five sequential SQL layers to process the data
Validation — Runs a balance check confirming Start MRR + Net New = End MRR for every period
Export — Saves the final datasets as CSVs to the outputs/ folder


🏗️ Project Structure
saas-mrr-analytics/
├── run_model.py                      # Python orchestration & validation script
├── sql/
│   ├── 01_stg_orders.sql             # JSON parsing, FX/tax normalisation, orphan removal
│   ├── 02_stg_subscriptions.sql      # Plan name normalisation & MRR attribution
│   ├── 03_int_customer_mrr.sql       # Recursive calendar spine & MRR fanning
│   ├── 04_mart_mrr_movements.sql     # Movement categorisation (New, Expansion, Contraction, Lost)
│   └── 05_mart_cohort_retention.sql  # Retention indexing by month number
└── outputs/                          # Final CSV exports (source for Power BI)

📊 Data Pipeline Architecture
Raw CSVs  →  Staging (stg_)  →  Intermediate (int_)  →  Mart (mart_)
LayerPrefixDescriptionStagingstg_Parses JSON metadata, handles currency conversion, normalises text, and removes orphaned ordersIntermediateint_Generates a recursive calendar spine to expand subscriptions into a monthly time-seriesMartmart_Aggregates MRR movement categories and computes cohort retention by month index

💡 Business Logic & Glossary
The following definitions are applied consistently across all SQL layers:
TermDefinitionMRRNet Revenue in EUR divided by 12 (annual billing cycle)Net RevenueGross amount minus tax, converted to EUR using the exchange rate from JSON metadataNew MRRRevenue from a customer with no MRR in the previous monthExpansion MRRIncrease in MRR for an existing customer compared to the previous monthContraction MRRDecrease in MRR (but still above zero) for an existing customerLost MRRFull revenue loss from a customer who had MRR last month but zero this monthCohort MonthThe first calendar month a customer generated any MRR

📥 Inputs
FileRowsDescriptionsubscriptions.csv709Raw subscription lifecycle dataorders.csv724Transactional data with JSON metadatausers.csv287Customer demographic and signup data (not used in MRR model)

📤 Outputs
FileGrainPurposemrr_movements.csvMonthHigh-level monthly health metrics (New vs Churn)cohort_retention.csvCohort × MonthLongitudinal retention and Net Revenue Retention (NRR)int_customer_mrr.csvCustomer × MonthCustomer-level grain — ideal for slicing in Power BI
