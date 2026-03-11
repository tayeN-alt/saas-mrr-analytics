/*
=========================================================================
FILE: int_customer_mrr.sql
LAYER: 2 - Intermediate
PURPOSE: Build a continuous monthly MRR timeline for every customer.

This is the backbone of the whole model. Before we can calculate any MRR
movements we need to know what every customer was paying in every single
month — and that's what this layer produces.

The approach has three steps: first we generate a full calendar spine of
every month in the data range, then we "fan out" each subscription across
its active months by joining to that spine, and finally we roll everything
up to the customer level in case a customer had multiple overlapping
subscriptions in the same month.

A few things worth knowing:
  - The calendar spine runs from 2023-01 to 2026-06, covering the full
    range of subscription activity in the dataset.
  - A subscription is considered active in a month if that month falls
    between first_mrr_month and last_mrr_month (both inclusive), which
    were already calculated and cleaned in stg_subscriptions.
  - plan_name is concatenated at the customer level so we can see if a
    customer was on multiple plans simultaneously — useful for debugging
    expansion and contraction movements later.
=========================================================================
*/

-- Generate every month from data start to end
CREATE VIEW calendar_months AS
WITH RECURSIVE months(m) AS (
    SELECT '2023-01'
    UNION ALL
    SELECT STRFTIME('%Y-%m', DATE(m || '-01', '+1 month'))
    FROM months
    WHERE m < '2026-06'
)
SELECT m AS calendar_month FROM months;

-- Expand each subscription across its active months
CREATE VIEW int_sub_months AS
SELECT
    s.subscription_id,
    s.customer_id,
    s.plan_name,
    s.mrr,
    c.calendar_month
FROM stg_subscriptions s
JOIN calendar_months c
  ON c.calendar_month >= s.first_mrr_month
 AND c.calendar_month <= s.last_mrr_month;

-- Aggregate to customer level
CREATE VIEW int_customer_mrr AS
SELECT
    customer_id,
    calendar_month,
    SUM(mrr) AS mrr,
    GROUP_CONCAT(DISTINCT plan_name) AS plans
FROM int_sub_months
GROUP BY customer_id, calendar_month;