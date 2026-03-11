# SaaS MRR Analytics

A lightweight, self-contained data pipeline that transforms raw B2B SaaS subscription data into MRR Movements and Cohort Retention metrics.

For architecture decisions, data quality findings, and business insights — see [DOCUMENTATION.md](DOCUMENTATION.md).

---

## Requirements

- Python 3.8+
- pandas

No other dependencies. SQLite is built into Python.

---

## How to Run

```bash
python run_model.py
```

The script runs four steps in order:

1. **Ingestion** — loads three raw CSVs into an in-memory SQLite database
2. **Transformation** — executes five sequential SQL layers
3. **Validation** — balance check confirming `Start MRR + Net New = End MRR` for every period
4. **Export** — saves final datasets as CSVs to the `outputs/` folder

---

## Project Structure

```
saas-mrr-analytics/
├── run_model.py                      # Orchestration & validation
├── sql/
│   ├── 01_stg_orders.sql             # JSON parsing, FX/tax normalisation, orphan removal
│   ├── 02_stg_subscriptions.sql      # Plan name normalisation & MRR attribution
│   ├── 03_int_customer_mrr.sql       # Recursive calendar spine & MRR fanning
│   ├── 04_mart_mrr_movements.sql     # Movement categorisation (New, Expansion, Contraction, Lost)
│   └── 05_mart_cohort_retention.sql  # Retention indexing by month number
├── data/                             # Raw input CSVs
├── outputs/                          # Final CSV exports
└── DOCUMENTATION.md                  # Architecture decisions, data findings & business insights
```

---

## Data Flow

```
Raw CSVs  →  Staging (stg_)  →  Intermediate (int_)  →  Mart (mart_)
```

| Layer | Prefix | Description |
|---|---|---|
| Staging | `stg_` | Parses JSON metadata, handles currency conversion, normalises text, removes orphaned orders |
| Intermediate | `int_` | Generates a recursive calendar spine to expand subscriptions into a monthly time-series |
| Mart | `mart_` | Aggregates MRR movement categories and computes cohort retention by month index |

---

## Business Logic & Glossary

| Term | Definition |
|---|---|
| MRR | Net Revenue in EUR divided by 12 (annual billing cycle) |
| Net Revenue | Gross amount minus tax, converted to EUR using the exchange rate from JSON metadata |
| New MRR | Revenue from a customer with no MRR in the previous month |
| Expansion MRR | Increase in MRR for an existing customer vs the previous month |
| Contraction MRR | Decrease in MRR (but still above zero) for an existing customer |
| Lost MRR | Full revenue loss from a customer who had MRR last month but zero this month |
| Cohort Month | The first calendar month a customer generated any MRR |

---

## Inputs

| File | Rows | Description |
|---|---|---|
| `subscriptions.csv` | 709 | Raw subscription lifecycle data |
| `orders.csv` | 724 | Transactional data with JSON metadata |
| `users.csv` | 287 | Customer demographic and signup data (not used in MRR model) |

---

## Outputs

| File | Grain | Description |
|---|---|---|
| `mrr_movements.csv` | Month | Monthly MRR movements — New, Expansion, Contraction, Lost, Start and End of Period |
| `cohort_retention.csv` | Cohort × Month | Retained MRR and retention % by cohort and month number |
| `int_customer_mrr.csv` | Customer × Month | Customer-level MRR grain |
