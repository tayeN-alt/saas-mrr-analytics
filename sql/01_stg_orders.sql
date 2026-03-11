/*
=========================================================================
FILE: stg_orders.sql
LAYER: 1 - Staging
PURPOSE: Clean and prepare raw order data for downstream MRR calculations.

We start here because the orders table holds the money — but the revenue
figures are buried in a JSON column and haven't been cleaned yet. This
view unpacks that metadata, strips out tax, and converts everything to
EUR so the rest of the pipeline works in a single consistent currency.

A few things worth knowing:
  - 2 orders have no matching subscription and are dropped early here
    rather than letting them cause silent errors downstream.
  - Some orders have no tax_percentage in the JSON — we treat those as
    tax-free (0%) rather than rejecting them.
  - exchange_rate is guarded against zero/null to avoid division errors;
    those rows return NULL for net_eur so they're visible and traceable.
  - All monetary outputs are rounded to 2 decimal places to avoid
    floating point noise accumulating through the pipeline.
=========================================================================
*/

CREATE VIEW stg_orders AS
WITH raw AS (
    SELECT
        o.order_id,
        o.subscription_id,
        DATE(o.order_date) AS order_date,
        o.gross_amount,
        json_extract(o.checkout_metadata, '$.currency') AS currency,
        CAST(json_extract(o.checkout_metadata, '$.exchange_rate') AS REAL) AS exchange_rate,
        COALESCE(
            CAST(json_extract(o.checkout_metadata, '$.tax_percentage') AS REAL),
            0.0
        ) AS tax_rate
    FROM orders o
    WHERE o.subscription_id IN (SELECT subscription_id FROM subscriptions)
),
cleaned AS (
    SELECT
        order_id,
        subscription_id,
        order_date,
        gross_amount,
        currency,
        exchange_rate,
        tax_rate,
        ROUND(gross_amount / (1.0 + tax_rate), 2) AS net_local,
        CASE 
            WHEN exchange_rate > 0 
            THEN ROUND((gross_amount / (1.0 + tax_rate)) / exchange_rate, 2)
            ELSE NULL 
        END AS net_eur
    FROM raw
)
SELECT * FROM cleaned;
