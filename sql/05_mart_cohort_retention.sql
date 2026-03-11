/*
=========================================================================
FILE: mart_cohort_retention.sql
LAYER: 3 - Mart
PURPOSE: Track how each customer cohort's MRR holds up over time.

A cohort here is simply the first month a customer had any MRR. From that
point we track whether their revenue stayed, grew, or disappeared — month
by month, indexed from zero. This tells us not just whether customers are
churning, but *when* they tend to churn and how much revenue is at risk.

The logic builds in three steps: first we pin each customer to their cohort,
then we map them onto every subsequent month using the calendar spine, and
finally we aggregate and compare back to what that cohort was worth at
month 0 to produce the retention percentage.

A few things worth knowing:
  - Month 0 is always 100% by definition — it's the baseline everything
    else is measured against.
  - julianday arithmetic is used for month indexing because SQLite doesn't
    support direct integer subtraction of STRFTIME strings.
  - NULLIF guards against division by zero in the retention percentage,
    returning NULL rather than crashing if a cohort has no month 0 MRR.
  - Cohorts near the end of the data range (late 2025) will only have a
    few months of history — their retention curves are naturally short.
=========================================================================
*/

-- Step 1: Assign each customer to their starting cohort
CREATE VIEW customer_cohorts AS
SELECT
    customer_id,
    MIN(calendar_month) AS cohort_month
FROM int_customer_mrr
WHERE mrr > 0
GROUP BY customer_id;

-- Step 2: Map customers to every month since joining
CREATE VIEW cohort_customer_months AS
SELECT
    cc.customer_id,
    cc.cohort_month,
    cm.calendar_month,
    CAST(ROUND(
        (julianday(cm.calendar_month || '-01') - julianday(cc.cohort_month || '-01')) / 30.4375
    ) AS INTEGER) AS month_number,
    COALESCE(icm.mrr, 0) AS mrr
FROM customer_cohorts cc
JOIN calendar_months cm
  ON cm.calendar_month >= cc.cohort_month
JOIN int_customer_mrr icm
  ON icm.customer_id = cc.customer_id
 AND icm.calendar_month = cm.calendar_month;

-- Step 3: Aggregate cohort metrics and calculate retention %
CREATE VIEW mart_cohort_retention AS
WITH cohort_base AS (
    SELECT
        cohort_month,
        SUM(mrr) AS cohort_mrr_month0,
        COUNT(DISTINCT customer_id) AS cohort_size
    FROM cohort_customer_months
    WHERE month_number = 0
    GROUP BY cohort_month
)
SELECT
    ccm.cohort_month,
    ccm.month_number,
    COUNT(DISTINCT ccm.customer_id) AS retained_customers,
    ROUND(SUM(ccm.mrr), 2) AS retained_mrr,
    cb.cohort_mrr_month0,
    cb.cohort_size,
    ROUND(
        100.0 * SUM(ccm.mrr) / NULLIF(cb.cohort_mrr_month0, 0),
    1) AS mrr_retention_pct
FROM cohort_customer_months ccm
JOIN cohort_base cb ON cb.cohort_month = ccm.cohort_month
GROUP BY ccm.cohort_month, ccm.month_number
ORDER BY ccm.cohort_month, ccm.month_number;
