/*
=========================================================================
FILE: stg_subscriptions.sql
LAYER: 1 - Staging
PURPOSE: Normalise subscriptions and attach MRR to each one.

This view is where each subscription gets its MRR value — calculated as
net EUR revenue divided by 12, since every order represents exactly one
annual term. It also handles a few data quality issues we found in the
raw table so nothing dirty reaches the intermediate layer.

A few things worth knowing:
  - plan_name had inconsistent casing in the source ('starter' vs
    'Starter') — normalised here so grouping and filtering work cleanly.
  - 2 subscriptions had 0 licenses, which would make no business sense.
    We floor those at 1 rather than dropping the rows entirely.
  - first_mrr_month and last_mrr_month define the active window for MRR.
    The end month is excluded per the business rules, so we subtract one
    month from end_date before extracting the year-month.
  - MRR is rounded to 2 decimal places to stay consistent with the
    rounding applied in stg_orders.
=========================================================================
*/

CREATE VIEW stg_subscriptions AS
SELECT
    s.subscription_id,
    s.customer_id,
    UPPER(SUBSTR(TRIM(s.plan_name), 1, 1)) || LOWER(SUBSTR(TRIM(s.plan_name), 2))
        AS plan_name,
    CASE 
        WHEN s.number_of_licenses < 1 THEN 1
        ELSE s.number_of_licenses 
    END AS number_of_licenses,
    DATE(s.start_date) AS start_date,
    DATE(s.end_date) AS end_date,
    STRFTIME('%Y-%m', s.start_date) AS first_mrr_month,
    STRFTIME('%Y-%m', DATE(s.end_date, 'start of month', '-1 month')) 
        AS last_mrr_month,
    ROUND(o.net_eur / 12.0, 2) AS mrr
FROM subscriptions s
JOIN stg_orders o USING (subscription_id);
