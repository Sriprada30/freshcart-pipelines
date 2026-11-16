"""
Core FreshCart ELT logic, shared by the Part B notebook and the Part D bonus
(stream -> lakehouse) consumer.

IMPORTANT: this was written without the original course notebook/dataset (not
available to the author of this scaffold). The mock orders below are a
self-consistent reconstruction that reproduces the SAME STRUCTURE the
assignment describes (8 raw rows = 6 web + 2 mobile, 6 pass cleaning, 2
quarantined, 4 after dedup) so every exercise in Part B is answerable — but
the dollar amounts / loyalty points are from this mock dataset, not the
instructor's original numbers. Run it and report what YOUR run produces.

Design of the mock data (orders/customers), by construction:
  - CUST-01 Priya:  Silver from 2026-01-01, upgraded to Gold from 2026-07-05.
      -> ORD-1001 (2026-07-01) prices at Silver; ORD-1005 (2026-07-08) at Gold.
  - CUST-02 Arjun:  Bronze from 2026-01-01. B4 adds a Gold event effective
      2026-08-15, which is AFTER ORD-1002's order_date, so the tier *used for
      ORD-1002* should NOT change -- that's the SCD2 point-in-time lesson.
  - CUST-03 Rahul:  ORD-1003 arrives via web (amount missing) and mobile
      (amount filled, later updated_at) -- the B2 trace example.
  - CUST-04 Sana:   ORD-1004, amount missing on web -> quarantined.
  - CUST-05 Divya:  ORD-1006, negative amount + EUR currency -> quarantined
      (negative_amount from the start; unsupported_currency added in B3).
  - CUST-01 Priya also has a second (mobile, redundant) arrival for ORD-1001,
      which is the second of the "2 mobile" rows.
"""
import json
import os
from datetime import datetime

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

LANDING_DIR = os.path.join(os.path.dirname(__file__), "_landing")
os.makedirs(LANDING_DIR, exist_ok=True)

DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "FRESHCART")

# ---------------------------------------------------------------------------
# Mock "API" extract — in production this would be an HTTP call per source.
# ---------------------------------------------------------------------------

WEB_ORDERS = [
    # order_id, customer_id, amount, currency, order_date, updated_at
    ("ORD-1001", "CUST-01", 1500.00, "INR", "2026-07-01", "2026-07-01T10:00:00"),
    ("ORD-1002", "CUST-02", 2200.00, "INR", "2026-07-02", "2026-07-02T11:30:00"),
    ("ORD-1003", "CUST-03", None, "INR", "2026-07-03", "2026-07-03T09:15:00"),
    ("ORD-1004", "CUST-04", None, "INR", "2026-07-04", "2026-07-04T14:00:00"),
    ("ORD-1005", "CUST-01", 3200.00, "INR", "2026-07-08", "2026-07-08T16:45:00"),
    ("ORD-1006", "CUST-05", -500.00, "EUR", "2026-07-05", "2026-07-05T12:00:00"),
]

# Mobile app uses different field names for the same concepts -- cust_id vs
# customer_id, amt vs amount, order_ts vs order_date -- which is exactly why
# SQL_10_ORDERS_CLEAN needs two parsing branches (B2).
MOBILE_ORDERS = [
    # order_id, cust_id, amt, currency, order_ts, updated_at
    ("ORD-1003", "CUST-03", 2100.00, "INR", "2026-07-03", "2026-07-03T18:40:00"),
    ("ORD-1001", "CUST-01", 1500.00, "INR", "2026-07-01", "2026-07-01T10:05:00"),
]

CUSTOMERS = {
    "CUST-01": "Priya Sharma",
    "CUST-02": "Arjun Mehta",
    "CUST-03": "Rahul Verma",
    "CUST-04": "Sana Iyer",
    "CUST-05": "Divya Nair",
}

# Initial CRM tier history (SCD2 source). B4 appends one more row for CUST-02.
MOCK_CRM_TIER_EVENTS = [
    # customer_id, tier, effective_from
    ("CUST-01", "Silver", "2026-01-01"),
    ("CUST-01", "Gold", "2026-07-05"),
    ("CUST-02", "Bronze", "2026-01-01"),
    ("CUST-03", "Silver", "2026-01-01"),
    ("CUST-04", "Bronze", "2026-01-01"),
    ("CUST-05", "Bronze", "2026-01-01"),
    # Bonus: streamed orders use this synthetic customer -- without a matching
    # SCD2 row, the gold MERGE's inner join silently drops them (found by
    # actually running the bonus consumer and seeing gold NOT grow at all).
    ("CUST-STREAM", "Bronze", "2020-01-01"),
]


def extract_to_landing() -> dict:
    """Write the mock web/mobile 'API responses' to local landing files."""
    web_path = os.path.join(LANDING_DIR, "web_orders.json")
    mobile_path = os.path.join(LANDING_DIR, "mobile_orders.json")

    web_records = [
        {
            "order_id": o[0], "customer_id": o[1], "amount": o[2],
            "currency": o[3], "order_date": o[4], "updated_at": o[5],
        }
        for o in WEB_ORDERS
    ]
    mobile_records = [
        {
            "order_id": o[0], "cust_id": o[1], "amt": o[2],
            "currency": o[3], "order_ts": o[4], "updated_at": o[5],
        }
        for o in MOBILE_ORDERS
    ]

    with open(web_path, "w") as f:
        for r in web_records:
            f.write(json.dumps(r) + "\n")
    with open(mobile_path, "w") as f:
        for r in mobile_records:
            f.write(json.dumps(r) + "\n")

    return {"web": web_path, "mobile": mobile_path}


def write_landing(records: list, source: str) -> str:
    """Bonus: write a batch of streamed FreshCart-style order dicts to a landing file."""
    path = os.path.join(LANDING_DIR, f"stream_{source}_{int(datetime.utcnow().timestamp())}.json")
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    return path


# ---------------------------------------------------------------------------
# Snowflake connection + DDL
# ---------------------------------------------------------------------------

def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )


DDL = f"""
CREATE DATABASE IF NOT EXISTS {DATABASE};
CREATE SCHEMA IF NOT EXISTS {DATABASE}.BRONZE;
CREATE SCHEMA IF NOT EXISTS {DATABASE}.SILVER;
CREATE SCHEMA IF NOT EXISTS {DATABASE}.DIM;
CREATE SCHEMA IF NOT EXISTS {DATABASE}.REF;
CREATE SCHEMA IF NOT EXISTS {DATABASE}.GOLD;

CREATE OR REPLACE FILE FORMAT {DATABASE}.BRONZE.JSON_LINES
  TYPE = JSON STRIP_OUTER_ARRAY = FALSE;

CREATE STAGE IF NOT EXISTS {DATABASE}.BRONZE.LANDING_STAGE
  FILE_FORMAT = (FORMAT_NAME = '{DATABASE}.BRONZE.JSON_LINES');

CREATE TABLE IF NOT EXISTS {DATABASE}.BRONZE.ORDERS_RAW (
  raw_json   VARIANT,
  source     STRING,
  loaded_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS {DATABASE}.REF.LOYALTY_MULTIPLIERS (
  tier       STRING,
  multiplier FLOAT
);

CREATE TABLE IF NOT EXISTS {DATABASE}.DIM.CUSTOMERS_SCD2 (
  customer_id    STRING,
  tier           STRING,
  effective_from DATE,
  effective_to   DATE,
  is_current     BOOLEAN
);

CREATE TABLE IF NOT EXISTS {DATABASE}.GOLD.FCT_ORDERS (
  order_id        STRING,
  customer_id     STRING,
  amount_inr      FLOAT,
  currency        STRING,
  order_date      DATE,
  tier_used       STRING,
  loyalty_points  NUMBER,
  row_hash        STRING,
  published_at    TIMESTAMP_NTZ
);
"""


def run_ddl(conn):
    for stmt in DDL.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.cursor().execute(stmt)

    conn.cursor().execute(f"""
        MERGE INTO {DATABASE}.REF.LOYALTY_MULTIPLIERS t
        USING (
            SELECT * FROM VALUES
                ('Bronze', 1.0), ('Silver', 1.5), ('Gold', 2.0)
            AS v(tier, multiplier)
        ) s ON t.tier = s.tier
        WHEN NOT MATCHED THEN INSERT (tier, multiplier) VALUES (s.tier, s.multiplier);
    """)


# ---------------------------------------------------------------------------
# Load: PUT + COPY INTO bronze (FORCE=TRUE so bronze grows every run -- the
# raw landing layer is deliberately NOT deduplicated; see B4's explanation).
# ---------------------------------------------------------------------------

def load_file(conn, local_path: str, source: str):
    cur = conn.cursor()
    cur.execute(f"PUT file://{local_path} @{DATABASE}.BRONZE.LANDING_STAGE OVERWRITE = TRUE")
    filename = os.path.basename(local_path)
    cur.execute(f"""
        COPY INTO {DATABASE}.BRONZE.ORDERS_RAW (raw_json, source)
        FROM (
            SELECT $1, '{source}'
            FROM @{DATABASE}.BRONZE.LANDING_STAGE/{filename}
        )
        FILE_FORMAT = (FORMAT_NAME = {DATABASE}.BRONZE.JSON_LINES)
        FORCE = TRUE;
    """)


def run_baseline_load(conn):
    paths = extract_to_landing()
    load_file(conn, paths["web"], "web")
    load_file(conn, paths["mobile"], "mobile")


# ---------------------------------------------------------------------------
# Transform: bronze -> silver (clean + quarantine) -> dedup -> gold
# ---------------------------------------------------------------------------

# SQL_10_ORDERS_CLEAN -- parses both source schemas into common columns and
# builds the `reasons` array. B1 (baseline) runs this WITHOUT the currency
# line; B3 asks you to add it yourself and compare reasons before/after, so
# the line is toggled by `include_currency_check` rather than always on.
def sql_10_orders_clean(include_currency_check: bool = False) -> str:
    reasons = [
        "IFF(amount IS NULL, 'missing_amount', NULL)",
        "IFF(amount IS NOT NULL AND amount < 0, 'negative_amount', NULL)",
    ]
    if include_currency_check:
        reasons.append("IFF(currency NOT IN ('INR', 'USD'), 'unsupported_currency', NULL)")
    reasons_sql = ",\n        ".join(reasons)
    return f"""
CREATE OR REPLACE TABLE {DATABASE}.SILVER.ORDERS_CHECKED AS
WITH unioned AS (
    -- web schema: order_id, customer_id, amount, currency, order_date, updated_at
    SELECT
        raw_json:order_id::STRING    AS order_id,
        raw_json:customer_id::STRING AS customer_id,
        raw_json:amount::FLOAT       AS amount,
        raw_json:currency::STRING    AS currency,
        raw_json:order_date::DATE    AS order_date,
        raw_json:updated_at::TIMESTAMP_NTZ AS updated_at,
        source
    FROM {DATABASE}.BRONZE.ORDERS_RAW
    WHERE source = 'web'

    UNION ALL

    -- mobile schema: order_id, cust_id, amt, currency, order_ts, updated_at
    SELECT
        raw_json:order_id::STRING    AS order_id,
        raw_json:cust_id::STRING     AS customer_id,
        raw_json:amt::FLOAT          AS amount,
        raw_json:currency::STRING    AS currency,
        raw_json:order_ts::DATE      AS order_date,
        raw_json:updated_at::TIMESTAMP_NTZ AS updated_at,
        source
    FROM {DATABASE}.BRONZE.ORDERS_RAW
    WHERE source = 'mobile'
)
SELECT
    *,
    ARRAY_CONSTRUCT_COMPACT(
        {reasons_sql}
    ) AS reasons
FROM unioned;
"""

# SQL_20_ORDERS_DEDUP -- keep only clean rows (empty reasons array), one row
# per order_id, preferring the most recently updated version.
SQL_20_ORDERS_DEDUP = f"""
CREATE OR REPLACE TABLE {DATABASE}.SILVER.ORDERS_DEDUP AS
SELECT * FROM (
    SELECT *
    FROM {DATABASE}.SILVER.ORDERS_CHECKED
    WHERE ARRAY_SIZE(reasons) = 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1
);
"""


def run_transform(conn, include_currency_check: bool = False):
    conn.cursor().execute(sql_10_orders_clean(include_currency_check))
    conn.cursor().execute(SQL_20_ORDERS_DEDUP)
    _refresh_scd2(conn)
    _merge_gold(conn)


def get_reasons_for_order(conn, order_id: str) -> list:
    """Helper for B3: inspect ORD-1006's reasons array before/after your change."""
    cur = conn.cursor()
    cur.execute(f"""
        SELECT source, reasons FROM {DATABASE}.SILVER.ORDERS_CHECKED
        WHERE order_id = %s
    """, (order_id,))
    return cur.fetchall()


def query_df(conn, sql: str) -> pd.DataFrame:
    """Run a SELECT and return the result as a DataFrame (for notebook display)."""
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetch_pandas_all()


def _refresh_scd2(conn, extra_events: list = None):
    """Rebuild dim.customers_scd2 from MOCK_CRM_TIER_EVENTS (+ any extra B4 events)."""
    events = list(MOCK_CRM_TIER_EVENTS)
    if extra_events:
        events.extend(extra_events)
    events.sort(key=lambda e: (e[0], e[2]))

    rows = []
    by_customer = {}
    for customer_id, tier, eff_from in events:
        by_customer.setdefault(customer_id, []).append((tier, eff_from))

    for customer_id, history in by_customer.items():
        for i, (tier, eff_from) in enumerate(history):
            eff_to = history[i + 1][1] if i + 1 < len(history) else "9999-12-31"
            is_current = i + 1 == len(history)
            rows.append((customer_id, tier, eff_from, eff_to, is_current))

    cur = conn.cursor()
    cur.execute(f"DELETE FROM {DATABASE}.DIM.CUSTOMERS_SCD2")
    cur.executemany(
        f"INSERT INTO {DATABASE}.DIM.CUSTOMERS_SCD2 "
        f"(customer_id, tier, effective_from, effective_to, is_current) VALUES (%s, %s, %s, %s, %s)",
        rows,
    )


# row_hash encodes every business column, so it doubles as the natural MERGE
# key: if nothing about an order changed, its row_hash is identical on the
# next run and the MERGE just no-ops (or touches published_at) instead of
# inserting a duplicate row -- that's what makes re-runs idempotent.
SQL_30_GOLD_MERGE = f"""
MERGE INTO {DATABASE}.GOLD.FCT_ORDERS t
USING (
    SELECT
        d.order_id,
        d.customer_id,
        IFF(d.currency = 'USD', d.amount * 83.0, d.amount) AS amount_inr,
        d.currency,
        d.order_date,
        s.tier AS tier_used,
        FLOOR(IFF(d.currency = 'USD', d.amount * 83.0, d.amount) / 100 * m.multiplier) AS loyalty_points,
        MD5(
            d.order_id || '|' || d.customer_id || '|' ||
            TO_VARCHAR(IFF(d.currency = 'USD', d.amount * 83.0, d.amount)) || '|' ||
            s.tier
        ) AS row_hash
    FROM {DATABASE}.SILVER.ORDERS_DEDUP d
    JOIN {DATABASE}.DIM.CUSTOMERS_SCD2 s
      ON s.customer_id = d.customer_id
     AND d.order_date >= s.effective_from
     AND d.order_date < s.effective_to
    JOIN {DATABASE}.REF.LOYALTY_MULTIPLIERS m
      ON m.tier = s.tier
) src
ON t.row_hash = src.row_hash
WHEN MATCHED THEN UPDATE SET published_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (order_id, customer_id, amount_inr, currency, order_date,
                               tier_used, loyalty_points, row_hash, published_at)
VALUES (src.order_id, src.customer_id, src.amount_inr, src.currency, src.order_date,
        src.tier_used, src.loyalty_points, src.row_hash, CURRENT_TIMESTAMP());
"""


def _merge_gold(conn):
    conn.cursor().execute(SQL_30_GOLD_MERGE)


# ---------------------------------------------------------------------------
# B4 — add a CRM tier event and re-run
# ---------------------------------------------------------------------------

def add_tier_event_and_rerun(conn, customer_id: str, tier: str, effective_from: str):
    _refresh_scd2(conn, extra_events=[(customer_id, tier, effective_from)])
    _merge_gold(conn)


# ---------------------------------------------------------------------------
# quality_checks() -- B1's baseline checks + B5's new sixth check
# ---------------------------------------------------------------------------

def quality_checks(conn, include_future_date_check: bool = True) -> dict:
    cur = conn.cursor()
    checks = {}

    cur.execute(f"""
        SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM {DATABASE}.GOLD.FCT_ORDERS
    """)
    checks["duplicate_orders_in_gold"] = cur.fetchone()[0]

    cur.execute(f"""
        SELECT COUNT(*) FROM {DATABASE}.GOLD.FCT_ORDERS WHERE loyalty_points < 0
    """)
    checks["negative_loyalty_points"] = cur.fetchone()[0]

    cur.execute(f"""
        SELECT COUNT(*) FROM {DATABASE}.GOLD.FCT_ORDERS WHERE tier_used IS NULL
    """)
    checks["orders_missing_tier"] = cur.fetchone()[0]

    cur.execute(f"""
        SELECT COUNT(*) FROM {DATABASE}.SILVER.ORDERS_CHECKED
        WHERE ARRAY_SIZE(reasons) = 0 AND amount IS NULL
    """)
    checks["clean_rows_with_null_amount"] = cur.fetchone()[0]

    cur.execute(f"""
        SELECT COUNT(*) FROM {DATABASE}.SILVER.ORDERS_CHECKED
        WHERE ARRAY_SIZE(reasons) > 0 AND amount IS NOT NULL AND amount >= 0 AND currency IN ('INR', 'USD')
    """)
    checks["quarantined_rows_with_no_real_reason"] = cur.fetchone()[0]

    if include_future_date_check:
        # B5: orders in silver.orders_dedup whose order_date is in the future
        cur.execute(f"""
            SELECT COUNT(*) FROM {DATABASE}.SILVER.ORDERS_DEDUP
            WHERE order_date > CURRENT_DATE()
        """)
        checks["future_dated_orders"] = cur.fetchone()[0]

    return checks
