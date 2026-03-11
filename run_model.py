"""
=========================================================================
FILE: run_model.py
PURPOSE: Orchestration script for the SaaS MRR Analytics pipeline.

This script is the single entry point for the entire pipeline. It handles
ingestion, transformation, validation, and export in one clean run. The
SQL layers do all the heavy lifting — this script just loads the data,
fires them in the right order, and checks the output makes sense.

Steps:
  1. Ingestion       — Loads three raw CSVs into an in-memory SQLite database
  2. Transformation  — Executes five SQL layers (Staging → Intermediate → Mart)
  3. Export          — Saves final datasets as CSVs to the outputs/ folder
  4. Validation      — Confirms Start MRR + Net New = End MRR for every month
=========================================================================
"""

import sqlite3
import pandas as pd
import os
import sys
import time

# -------------------------------------------------------------------------
# Configuration — update these paths if your folder structure changes
# -------------------------------------------------------------------------
DB       = ":memory:"   # In-memory SQLite — no file left behind after the run
DATA_DIR = "data"       # Folder containing raw input CSVs
SQL_DIR  = "sql"        # Folder containing the .sql model files
OUT_DIR  = "outputs"    # Target folder for exported results


def load_csv_to_sqlite(conn, path, table):
    if not os.path.exists(path):
        print(f"ERROR: File not found: {path}")
        sys.exit(1)
    try:
        df = pd.read_csv(path)
        df.to_sql(table, conn, if_exists="replace", index=False)
        print(f"  Loaded {table} ({len(df):,} rows)")
    except Exception as e:
        print(f"ERROR: Failed loading {table}: {e}")
        sys.exit(1)


def run_sql_file(conn, path):
    # Uses executescript() rather than splitting on semicolons — safer for
    # files that contain comments or strings with semicolons inside them.
    if not os.path.exists(path):
        print(f"ERROR: SQL file not found: {path}")
        sys.exit(1)
    try:
        with open(path, "r", encoding="utf-8") as f:
            sql = f.read()
        conn.executescript(sql)
    except Exception as e:
        print(f"ERROR in {os.path.basename(path)}")
        print(f"Details: {e}")
        sys.exit(1)


def main():

    start_time = time.time()

    print("\nSaaS MRR Analytics Pipeline")
    print("=" * 45)

    with sqlite3.connect(DB) as conn:

        # -----------------------------------------------------------------
        # 1. Ingestion
        # -----------------------------------------------------------------
        print("\n[1] Ingesting raw data")
        load_csv_to_sqlite(conn, os.path.join(DATA_DIR, "users.csv"),         "users")
        load_csv_to_sqlite(conn, os.path.join(DATA_DIR, "subscriptions.csv"), "subscriptions")
        load_csv_to_sqlite(conn, os.path.join(DATA_DIR, "orders.csv"),        "orders")

        # -----------------------------------------------------------------
        # 2. Transformation
        # -----------------------------------------------------------------
        # Order matters — each layer depends on the one before it:
        #   stg_orders → stg_subscriptions → int_customer_mrr → mart layers
        print("\n[2] Executing SQL layers")
        sql_files = [
            "01_stg_orders.sql",            # Staging:      parse JSON, compute net EUR
            "02_stg_subscriptions.sql",     # Staging:      normalise subscriptions, attach MRR
            "03_int_customer_mrr.sql",      # Intermediate: calendar spine + customer-month MRR
            "04_mart_mrr_movements.sql",    # Mart:         MRR movement categories
            "05_mart_cohort_retention.sql", # Mart:         cohort retention by month index
        ]
        for f in sql_files:
            print(f"  Running {f}")
            run_sql_file(conn, os.path.join(SQL_DIR, f))

        # -----------------------------------------------------------------
        # 3. Export
        # -----------------------------------------------------------------
        print("\n[3] Exporting results")
        os.makedirs(OUT_DIR, exist_ok=True)

        exports = [
            ("mart_mrr_movements",    "mrr_movements.csv"),
            ("mart_cohort_retention", "cohort_retention.csv"),
            ("int_customer_mrr",      "int_customer_mrr.csv"),
        ]
        for view, fname in exports:
            try:
                df = pd.read_sql(f"SELECT * FROM {view}", conn)
                df.to_csv(os.path.join(OUT_DIR, fname), index=False)
                print(f"  Exported {fname} ({len(df):,} rows)")
            except Exception as e:
                print(f"ERROR: Failed exporting {view}")
                print(f"Details: {e}")
                sys.exit(1)

        # -----------------------------------------------------------------
        # 4. Validation
        # -----------------------------------------------------------------
        # Checks that the MRR waterfall balances for every month.
        # The formula must hold: Start of Period + Net New MRR = End of Period
        # A tolerance of 0.01 accounts for minor floating point rounding.
        print("\n[4] Running integrity checks")
        try:
            mrr = pd.read_sql("SELECT * FROM mart_mrr_movements", conn)
            mrr["diff"] = (
                mrr["end_of_period_mrr"]
                - (mrr["start_of_period_mrr"] + mrr["net_new_mrr"])
            ).abs()

            errors = mrr[mrr["diff"] > 0.01]

            if len(errors) == 0:
                print("  Integrity check passed. MRR balances for all months.")
            else:
                print(f"  Integrity check failed. {len(errors)} month(s) imbalanced.\n")
                print(
                    errors[[
                        "calendar_month",
                        "start_of_period_mrr",
                        "end_of_period_mrr",
                        "net_new_mrr",
                        "diff",
                    ]].to_string(index=False)
                )
        except Exception as e:
            print(f"ERROR during validation: {e}")
            sys.exit(1)

    runtime = round(time.time() - start_time, 2)
    print("\n" + "=" * 45)
    print(f"Pipeline completed in {runtime} seconds")
    print("Outputs saved to /outputs\n")


if __name__ == "__main__":
    main()
