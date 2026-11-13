#!/usr/bin/env python3
"""Unit tests for B2B Merchant Enrichment Completeness & Ledger Reconciliation Audit."""
from __future__ import annotations

import csv
import json
import sys
import unittest
from pathlib import Path


MODULE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULE_DIR))

import enrichment_quality_pipeline as eqp  # noqa: E402


class TestEnrichmentQualityAudit(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if not (eqp.OUTPUT_DIR / "enrichment_validation_summary.json").is_file():
            eqp.run_enrichment_audit()
        cls.summary = json.loads((eqp.OUTPUT_DIR / "enrichment_validation_summary.json").read_text(encoding="utf-8"))
        with (eqp.OUTPUT_DIR / "merchant_enrichment_quality_mart.csv").open("r", encoding="utf-8") as handle:
            cls.mart = list(csv.DictReader(handle))

    def test_record_counts_and_fk_integrity(self) -> None:
        self.assertEqual(self.summary["orders_total"], 99441)
        self.assertEqual(self.summary["order_items_total"], 112650)
        self.assertEqual(self.summary["payment_rows_total"], 103886)
        self.assertEqual(self.summary["merchants_total"], 3095)
        self.assertEqual(self.summary["foreign_key_violations"], 0)

    def test_zero_ledger_variance(self) -> None:
        self.assertEqual(self.summary["ledger_variance_usd"], 0.0)
        self.assertGreater(self.summary["total_settled_payments"], 16000000.0)

    def test_state_mart_conservation(self) -> None:
        self.assertEqual(sum(int(r["merchant_count"]) for r in self.mart), 3095)
        self.assertEqual(sum(int(r["total_line_items"]) for r in self.mart), 112650)

    def test_enrichment_bounds_and_anomalies(self) -> None:
        self.assertEqual(self.summary["orders_missing_items"], 775)
        self.assertEqual(self.summary["orders_missing_payments"], 1)
        for row in self.mart:
            pct = float(row["catalog_enrichment_pct"])
            self.assertGreaterEqual(pct, 0.0)
            self.assertLessEqual(pct, 100.0)


if __name__ == "__main__":
    unittest.main()
