/*
=========================================================================
FILE: mart_mrr_movements.sql
LAYER: 3 - Mart
PURPOSE: Classify and aggregate monthly MRR changes into movement categories.

This is the most complex file in the pipeline. For every customer in every
month we need to answer one question: compared to last month, did their MRR
go up, go down, appear from nothing, or disappear entirely? Those four
outcomes are what the business calls New, Expansion, Contraction, and Lost.

The tricky part is churn. A standard self-join only sees months where a
customer is active — it misses the month they leave entirely. So we use a
UNION ALL with a NOT EXISTS check to explicitly inject a churn row for any
customer who had MRR last month but has no record this month.

That UNION ALL introduces potential duplicates, so a de-duplication step
sits between the raw classifications and the final aggregation.

A few things worth knowing:
  - Contraction and Lost are stored as negative values so they naturally
    subtract when summed into net_new_mrr.
  - net_new_mrr is included as a sanity check — it should always equal
    end_of_period_mrr minus start_of_period_mrr.
  - The WHERE clause filters out any all-zero ghost rows that can appear
    at the edges of the calendar spine.
  - active_customers counts distinct customers with any MRR activity that
    month, not just new ones.
=========================================================================
*/

-- Step 1: Raw classifications
CREATE VIEW mrr_movements_raw AS
SELECT
    cur.customer_id,
    cur.calendar_month,
    COALESCE(prev.mrr, 0) AS mrr_start,
    cur.mrr AS mrr_end,
    CASE
        WHEN COALESCE(prev.mrr, 0) = 0 AND cur.mrr > 0
        THEN cur.mrr
        ELSE 0
    END AS new_mrr,
    CASE
        WHEN COALESCE(prev.mrr, 0) > 0 AND cur.mrr > COALESCE(prev.mrr, 0)
        THEN cur.mrr - COALESCE(prev.mrr, 0)
        ELSE 0
    END AS expansion_mrr,
    CASE
        WHEN COALESCE(prev.mrr, 0) > 0 AND cur.mrr > 0 AND cur.mrr < COALESCE(prev.mrr, 0)
        THEN cur.mrr - COALESCE(prev.mrr, 0)
        ELSE 0
    END AS contraction_mrr,
    CASE
        WHEN COALESCE(prev.mrr, 0) > 0 AND cur.mrr = 0
        THEN -COALESCE(prev.mrr, 0)
        ELSE 0
    END AS lost_mrr
FROM int_customer_mrr cur
LEFT JOIN int_customer_mrr prev
       ON cur.customer_id = prev.customer_id
      AND prev.calendar_month = STRFTIME('%Y-%m', DATE(cur.calendar_month || '-01', '-1 month'))

UNION ALL

SELECT
    prev.customer_id,
    STRFTIME('%Y-%m', DATE(prev.calendar_month || '-01', '+1 month')) AS calendar_month,
    prev.mrr AS mrr_start,
    0 AS mrr_end,
    0 AS new_mrr,
    0 AS expansion_mrr,
    0 AS contraction_mrr,
    -prev.mrr AS lost_mrr
FROM int_customer_mrr prev
WHERE NOT EXISTS (
    SELECT 1 FROM int_customer_mrr cur
    WHERE cur.customer_id = prev.customer_id
      AND cur.calendar_month = STRFTIME('%Y-%m', DATE(prev.calendar_month || '-01', '+1 month'))
)
AND STRFTIME('%Y-%m', DATE(prev.calendar_month || '-01', '+1 month')) <= '2026-06';

-- Step 2: De-duplicate
CREATE VIEW mrr_movements_deduped AS
SELECT
    customer_id,
    calendar_month,
    MAX(mrr_start) AS mrr_start,
    MAX(mrr_end) AS mrr_end,
    MAX(new_mrr) AS new_mrr,
    MAX(expansion_mrr) AS expansion_mrr,
    MIN(contraction_mrr) AS contraction_mrr,
    MIN(lost_mrr) AS lost_mrr
FROM mrr_movements_raw
GROUP BY customer_id, calendar_month;

-- Step 3: Final aggregated output
CREATE VIEW mart_mrr_movements AS
SELECT
    calendar_month,
    ROUND(SUM(mrr_start), 2) AS start_of_period_mrr,
    ROUND(SUM(new_mrr), 2) AS new_mrr,
    ROUND(SUM(expansion_mrr), 2) AS expansion_mrr,
    ROUND(SUM(contraction_mrr), 2) AS contraction_mrr,
    ROUND(SUM(lost_mrr), 2) AS lost_mrr,
    ROUND(SUM(mrr_end), 2) AS end_of_period_mrr,
    ROUND(SUM(new_mrr) + SUM(expansion_mrr) + SUM(contraction_mrr) + SUM(lost_mrr), 2) AS net_new_mrr,
    COUNT(DISTINCT customer_id) AS active_customers
FROM mrr_movements_deduped
WHERE mrr_end > 0 OR mrr_start > 0
GROUP BY calendar_month
ORDER BY calendar_month;
