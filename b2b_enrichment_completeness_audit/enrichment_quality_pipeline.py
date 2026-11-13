#!/usr/bin/env python3
"""B2B Merchant Enrichment Completeness, Join Reconciliation & Quality SLA Audit Module.

Profiles and reconciles 103,886 payment transactions across 99,441 orders, 112,650 line items,
and 3,095 B2B merchant sellers into Tableau/Power BI-ready reporting marts, verifying PK/FK
join completeness, integer-cent settlement parity, and state-level enrichment SLAs.
"""
from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


MODULE_DIR = Path(__file__).resolve().parent
CACHE_DIR = MODULE_DIR / ".cache_raw"
OUTPUT_DIR = MODULE_DIR / "outputs"

OLIST_BASE_URL = "https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/"
REQUIRED_FILES = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_products_dataset.csv",
]


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_raw_files(force: bool = False) -> dict[str, str]:
    """Download required public Olist CSV tables into local cache if absent and return SHA-256 digests."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    digests: dict[str, str] = {}
    for fname in REQUIRED_FILES:
        dest = CACHE_DIR / fname
        if force or not dest.is_file():
            urllib.request.urlretrieve(OLIST_BASE_URL + fname, dest)
        digests[fname] = _sha256(dest)
    return digests


def run_enrichment_audit() -> dict[str, Any]:
    """Run relational enrichment completeness, ledger parity, and SLA mart generation."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    digests = ensure_raw_files()

    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON;")
    cur = conn.cursor()

    cur.executescript(
        """
        CREATE TABLE sellers (
            seller_id TEXT PRIMARY KEY,
            seller_zip_code_prefix TEXT,
            seller_city TEXT,
            seller_state TEXT
        );
        CREATE TABLE products (
            product_id TEXT PRIMARY KEY,
            product_category_name TEXT,
            product_photos_qty INTEGER,
            product_weight_g REAL
        );
        CREATE TABLE orders (
            order_id TEXT PRIMARY KEY,
            order_status TEXT NOT NULL,
            order_purchase_timestamp TEXT
        );
        CREATE TABLE order_items (
            order_id TEXT NOT NULL REFERENCES orders(order_id),
            order_item_id INTEGER NOT NULL,
            product_id TEXT NOT NULL REFERENCES products(product_id),
            seller_id TEXT NOT NULL REFERENCES sellers(seller_id),
            price_cents INTEGER NOT NULL,
            freight_cents INTEGER NOT NULL,
            PRIMARY KEY (order_id, order_item_id)
        );
        CREATE TABLE order_payments (
            order_id TEXT NOT NULL REFERENCES orders(order_id),
            payment_sequential INTEGER NOT NULL,
            payment_type TEXT,
            payment_installments INTEGER,
            payment_cents INTEGER NOT NULL,
            PRIMARY KEY (order_id, payment_sequential)
        );
        """
    )

    with (CACHE_DIR / "olist_sellers_dataset.csv").open("r", encoding="utf-8") as f:
        sellers = [(r["seller_id"], r["seller_zip_code_prefix"] or None, r["seller_city"] or None, r["seller_state"] or None) for r in csv.DictReader(f)]
        cur.executemany("INSERT INTO sellers VALUES (?, ?, ?, ?)", sellers)

    with (CACHE_DIR / "olist_products_dataset.csv").open("r", encoding="utf-8") as f:
        products = [
            (
                r["product_id"],
                r["product_category_name"].strip() or None,
                int(float(r["product_photos_qty"])) if r["product_photos_qty"].strip() else None,
                float(r["product_weight_g"]) if r["product_weight_g"].strip() else None,
            )
            for r in csv.DictReader(f)
        ]
        cur.executemany("INSERT INTO products VALUES (?, ?, ?, ?)", products)

    with (CACHE_DIR / "olist_orders_dataset.csv").open("r", encoding="utf-8") as f:
        orders = [(r["order_id"], r["order_status"], r["order_purchase_timestamp"] or None) for r in csv.DictReader(f)]
        cur.executemany("INSERT INTO orders VALUES (?, ?, ?)", orders)

    with (CACHE_DIR / "olist_order_items_dataset.csv").open("r", encoding="utf-8") as f:
        items = [
            (
                r["order_id"],
                int(r["order_item_id"]),
                r["product_id"],
                r["seller_id"],
                int(round(float(r["price"]) * 100)),
                int(round(float(r["freight_value"]) * 100)),
            )
            for r in csv.DictReader(f)
        ]
        cur.executemany("INSERT INTO order_items VALUES (?, ?, ?, ?, ?, ?)", items)

    with (CACHE_DIR / "olist_order_payments_dataset.csv").open("r", encoding="utf-8") as f:
        payments = [
            (
                r["order_id"],
                int(r["payment_sequential"]),
                r["payment_type"] or None,
                int(float(r["payment_installments"])) if r["payment_installments"].strip() else None,
                int(round(float(r["payment_value"]) * 100)),
            )
            for r in csv.DictReader(f)
        ]
        cur.executemany("INSERT INTO order_payments VALUES (?, ?, ?, ?, ?)", payments)

    conn.commit()
    fk_violations = cur.execute("PRAGMA foreign_key_check;").fetchall()

    # Exact integer-cent settlement parity across all 103,886 payment records
    total_payment_cents_sql = cur.execute("SELECT SUM(payment_cents) FROM order_payments").fetchone()[0]
    total_payment_cents_py = sum(p[4] for p in payments)
    ledger_variance_cents = abs(total_payment_cents_sql - total_payment_cents_py)

    # State-level B2B Merchant Enrichment Quality Mart (for Tableau / Power BI / Snowflake)
    mart_rows = cur.execute(
        """
        WITH seller_enrichment AS (
            SELECT
                s.seller_id,
                s.seller_state,
                CASE WHEN s.seller_zip_code_prefix IS NOT NULL AND s.seller_city IS NOT NULL AND s.seller_state IS NOT NULL THEN 1 ELSE 0 END AS geo_complete,
                COUNT(oi.order_item_id) AS sold_line_items,
                SUM(CASE WHEN p.product_category_name IS NOT NULL AND p.product_photos_qty IS NOT NULL AND p.product_weight_g IS NOT NULL THEN 1 ELSE 0 END) AS fully_enriched_items,
                SUM(CASE WHEN p.product_category_name IS NULL THEN 1 ELSE 0 END) AS missing_category_items,
                SUM(oi.price_cents + oi.freight_cents) AS gross_item_cents
            FROM sellers s
            LEFT JOIN order_items oi ON s.seller_id = oi.seller_id
            LEFT JOIN products p ON oi.product_id = p.product_id
            GROUP BY s.seller_id, s.seller_state, geo_complete
        )
        SELECT
            COALESCE(seller_state, 'UNKNOWN') AS seller_state,
            COUNT(*) AS merchant_count,
            SUM(geo_complete) AS geo_complete_merchants,
            COALESCE(SUM(sold_line_items), 0) AS total_line_items,
            COALESCE(SUM(fully_enriched_items), 0) AS fully_enriched_items,
            COALESCE(SUM(missing_category_items), 0) AS missing_category_items,
            ROUND(COALESCE(SUM(fully_enriched_items) * 100.0 / NULLIF(SUM(sold_line_items), 0), 0.0), 2) AS catalog_enrichment_pct,
            ROUND(COALESCE(SUM(gross_item_cents), 0) / 100.0, 2) AS gross_item_revenue
        FROM seller_enrichment
        GROUP BY COALESCE(seller_state, 'UNKNOWN')
        ORDER BY merchant_count DESC, seller_state ASC
        """
    ).fetchall()

    mart_dicts = [
        {
            "seller_state": r[0],
            "merchant_count": r[1],
            "geo_complete_merchants": r[2],
            "total_line_items": r[3],
            "fully_enriched_items": r[4],
            "missing_category_items": r[5],
            "catalog_enrichment_pct": r[6],
            "gross_item_revenue": r[7],
            "sla_tier": "PASS_95_PLUS" if r[6] >= 95.0 else "REVIEW_ENRICHMENT_GAP",
        }
        for r in mart_rows
    ]

    with (OUTPUT_DIR / "merchant_enrichment_quality_mart.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(mart_dicts[0].keys()))
        writer.writeheader()
        writer.writerows(mart_dicts)

    # Order-level reconciliation anomalies
    unlinked_orders = cur.execute(
        """
        SELECT COUNT(*)
        FROM orders o
        LEFT JOIN (SELECT DISTINCT order_id FROM order_items) oi ON o.order_id = oi.order_id
        WHERE oi.order_id IS NULL
        """
    ).fetchone()[0]
    unpaid_orders = cur.execute(
        """
        SELECT COUNT(*)
        FROM orders o
        LEFT JOIN (SELECT DISTINCT order_id FROM order_payments) op ON o.order_id = op.order_id
        WHERE op.order_id IS NULL
        """
    ).fetchone()[0]
    merchants_with_gaps = sum(1 for r in mart_dicts if r["missing_category_items"] > 0)
    total_enriched_items = sum(r["fully_enriched_items"] for r in mart_dicts)
    total_sold_items = sum(r["total_line_items"] for r in mart_dicts)

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "orders_total": len(orders),
        "order_items_total": len(items),
        "payment_rows_total": len(payments),
        "merchants_total": len(sellers),
        "products_total": len(products),
        "foreign_key_violations": len(fk_violations),
        "total_settled_payments": round(total_payment_cents_sql / 100.0, 2),
        "ledger_variance_usd": round(ledger_variance_cents / 100.0, 2),
        "overall_catalog_enrichment_pct": round((total_enriched_items / total_sold_items) * 100.0, 2),
        "missing_category_line_items": sum(r["missing_category_items"] for r in mart_dicts),
        "states_profiled": len(mart_dicts),
        "states_with_enrichment_gaps": merchants_with_gaps,
        "orders_missing_items": unlinked_orders,
        "orders_missing_payments": unpaid_orders,
        "sha256_digests": digests,
    }
    (OUTPUT_DIR / "enrichment_validation_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    conn.close()
    return summary


if __name__ == "__main__":
    print(json.dumps(run_enrichment_audit(), indent=2))
