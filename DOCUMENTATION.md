# Project Documentation

## Overview

I built this assignment to transform raw subscription and order data into a functional MRR engine. I handled the entire pipeline — from ingestion and cleaning to calculating revenue movements and cohort retention — using SQL for the transformations and Python for orchestration.

---

## 1. Data Modeling & Architecture

I went with a layered architecture because it makes debugging much easier. If a number looks wrong in the final dashboard, I can trace it back through the layers to see exactly where the logic failed.

| Layer | Files | What it does |
|---|---|---|
| Staging | `01_stg_orders.sql`, `02_stg_subscriptions.sql` | Unpacks JSON metadata, removes tax, converts to EUR, normalises plan names, guards against bad data |
| Intermediate | `03_int_customer_mrr.sql` | Generates a recursive calendar spine and fans each subscription out into monthly slots at customer grain |
| Mart | `04_mart_mrr_movements.sql`, `05_mart_cohort_retention.sql` | The two business-facing outputs — MRR movements and cohort retention |

### Staging Layer
I started by unpacking the money. The orders table had revenue buried in a JSON column, so I extracted the currency, exchange rates, and tax percentage. I converted everything to EUR here so the rest of the pipeline could work in one consistent currency.

### Intermediate Layer
This is the backbone of the model. I created a calendar spine representing every month from 2023 to 2026, then joined subscriptions to it to fan out the annual revenue into 12 monthly slots. This step is what makes it possible to see silent churn — months where a customer should have been active but simply isn't.

### Mart Layer
Two views: one for monthly MRR movements (New, Expansion, Contraction, Lost) and one for Cohort Retention. I used a UNION ALL approach to force the model to see when a customer drops off, making sure churn is actually captured rather than silently ignored. A de-duplication step sits between the raw classifications and the final aggregation to prevent double-counting at the boundary.

---

## 2. Data Discoveries & Challenges

A few things in the data needed solving before the numbers could be trusted.

### The JSON Tax Problem
Not every order had a tax percentage in the metadata. I decided to treat missing values as 0% — tax-free — rather than dropping the rows. I also found two orders that didn't link to any subscription, so I dropped those early in staging before they could skew the totals.

### Casing Inconsistency
'Starter' and 'starter' were being treated as two different plans. I normalised everything to proper case in the staging layer so grouping and filtering would work correctly downstream.

### Zero License Subscriptions
A couple of subscriptions showed 0 licenses. Since a subscription with no users makes no business sense, I floored these at 1 so the rows stayed valid for revenue calculation rather than silently corrupting MRR.

### The 13th Month
Per the business rules, MRR had to stop exactly after 12 months unless a renewal was present. I handled this by calculating a `last_mrr_month` in staging, subtracting one month from the end date to keep the active window precise. Getting this boundary wrong would either over- or under-count revenue.

### Country Name Aliases
'US' vs 'USA' and 'DE' vs 'Germany' appear as separate values in the users table. This doesn't affect MRR since country isn't used in the model, but I flagged it because it would break any geographic analysis without normalisation first.

---

## 3. Business Insights

The data covers 42 months from January 2023 to June 2026 across 21 cohorts. Four datasets — MRR trajectory, movement breakdown, retained MRR, and retention % — tell a consistent and sobering story.

### Phase 1 — Pure Growth (Jan 2023 to Dec 2023)
The first 12 months were clean. Every single euro of new MRR came from new customers — zero churn, zero expansion, zero contraction. MRR grew from €70 in January to €6,100 by December 2023, and the customer base scaled from 2 to 204. December 2023 was the single biggest acquisition month at €1,170 in New MRR. The business hadn't yet had to deal with renewals, so this phase is almost entirely a function of sales motion.

### Phase 2 — First Renewals & Peak (Jan 2024 to Jul 2024)
January 2024 marked the first churn — €60 Lost MRR as the earliest 2023 subscribers hit their renewal. New MRR was still strong (€670 in January, €770 in February) which masked the churn signal. April 2024 was the first month with Expansion MRR (€80), confirming that at least some customers were adding licenses at renewal. MRR peaked at €8,760 in July 2024 with 289 active customers. Crucially, the customer count peaked in the same month as MRR — meaning the plateau wasn't driven by expansion from existing customers but by the last wave of new ones arriving.

### Phase 3 — Maturation & Slow Decline (Aug 2024 to Dec 2025)
New MRR effectively stopped after July 2024 — only three tiny months of acquisition in 17 months. The business was now running purely on renewals. Expansion MRR kept appearing (€140 in January 2025, €140 in June 2025) which cushioned the decline but couldn't offset Lost MRR. The worst single month in this phase was February 2025 at -€240 Lost. MRR drifted from €8,750 in August 2024 to €7,360 by December 2025 — a slow but consistent erosion of about €100 per month on average.

### Phase 4 — Renewal Cliff (Jan 2026 to Jun 2026)
January 2026 was a step change. Lost MRR jumped to -€720, then -€600 in February and -€830 in March — the three worst months in the entire dataset. No New MRR, no Expansion, no Contraction. Just churn. This is the 2023 cohorts — the largest cohorts by MRR — reaching their third renewal cycle and not returning. MRR fell from €7,360 in December 2025 to €3,670 by June 2026, a 50% decline in six months.

### Cohort Retention — The Real Signal
Across 20 cohorts with a visible month 12, the average first-year renewal rate was 85% and the median was 88.8% — both solid numbers. But the distribution tells a more complicated story.

Three cohorts expanded above 100% at first renewal: 2023-04 at 112.9%, 2023-10 at 109.8%, and 2024-06 at 128.6%. These customers didn't just renew — they added licenses. On the other end, five cohorts fell below 70%: the 2023-01 cohort retained just 14.3% at month 12, and the mid-2024 cohorts (April, May, July) all came in around 55–56%.

The second renewal (month 24) is where things get harder. Most cohorts dropped further — 2023-02 went from 81% to 61.9%, 2023-05 from 90.5% to 66.7%. The 2024-01 cohort is the most alarming: 97% at month 12, then a near-complete collapse to 1.5% at month 24. Two cohorts bucked the trend — 2023-03 grew to 112% and 2023-10 held at 102% at month 24 — suggesting that for some segments the product delivers increasing value over time.

One structural observation: August 2024 is completely absent from the cohort data. No subscriptions were signed in that month — worth flagging as either a sales gap or a data issue.

---

## 4. Tools & Tech Stack

| Tool | Why |
|---|---|
| SQLite | Zero dependencies, built into Python, handles recursive CTEs. In production I'd use DuckDB or BigQuery — but SQLite keeps the submission fully reproducible on any machine |
| Python + pandas | Single-command orchestration — ingests CSVs, fires SQL layers in order, runs the waterfall integrity check, exports results |
| Plain SQL scripts | Each file maps to one layer and can be read independently. Structure maps directly to dbt — each file would become a model with tests and documentation |
| Looker Studio | Browser-based, works on Mac, connects directly to CSV uploads, produces a shareable public link, handles pivot heatmaps well |

---

## 5. Dashboard

🔗 [View Dashboard]([https://lookerstudio.google.com/s/hPIl6k-evdw](https://lookerstudio.google.com/reporting/ad993485-e597-4de9-bd28-92c4f8da607f))

- **Page 1 — MRR Overview**: dual-axis line chart (MRR + active customers) and a combo bar chart showing New, Expansion, Contraction, and Lost MRR per month. Negative values for Contraction and Lost are intentional — they subtract from MRR in the waterfall logic.

- **Page 2 — Cohort Retention**: pivot heatmap filtered from month 12 onwards with retained MRR in EUR and a green-to-red colour scale, focusing on renewal behaviour rather than the flat 100% in-contract period.
  Retention % is added as an optional metric, you can select both metrics together or separately.

---

## 6. If I Had More Time

- **Migrate to dbt** — the file structure already maps directly to dbt models. Adding tests for unique keys, not-null constraints, and referential integrity would make the pipeline production-ready.
- **Make the calendar spine dynamic** — currently hardcoded to 2023-01 through 2026-06. Should derive boundaries from MIN/MAX subscription dates so it stays accurate as new data arrives.
- **Add a renewal rate KPI** — the cohort data shows retained MRR at month 12 but doesn't surface what % of customers actually renewed. Given the wide variance between cohorts (14.3% to 128.6%), that's the most important metric missing from the dashboard.
