import unittest
import sqlite3
from src.payment_pipeline import (
    wilson_ci,
    two_proportion_z_test,
    generate_olist_aligned_payment_ledger,
    execute_sql_transformations_and_qa_gates,
    run_qbr_mbr_and_experiment_analytics,
)


class TestPaymentPipelineAndStatisticalModels(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = sqlite3.connect(":memory:")
        cls.ingest_stats = generate_olist_aligned_payment_ledger(cls.conn, n_records=103886, seed=42)
        cls.qa_report = execute_sql_transformations_and_qa_gates(cls.conn)
        cls.scorecard, cls.exp_summary = run_qbr_mbr_and_experiment_analytics(cls.conn)

    def test_01_row_and_merchant_scale(self):
        self.assertEqual(self.ingest_stats["total_payment_rows"], 103886)
        self.assertEqual(self.ingest_stats["unique_orders"], 99441)
        self.assertEqual(self.ingest_stats["active_merchants"], 3095)

    def test_02_all_data_quality_assertions_pass(self):
        self.assertEqual(self.qa_report["total_checks_executed"], 8)
        self.assertEqual(self.qa_report["checks_passed"], 8)
        self.assertEqual(self.qa_report["pass_rate_pct"], 100.0)

    def test_03_wilson_confidence_interval_bounds(self):
        low, high = wilson_ci(318, 1000)
        self.assertGreater(low, 0.28)
        self.assertLess(high, 0.35)
        self.assertLess(low, high)

    def test_04_adaptive_retry_lift_statistical_significance(self):
        inf = self.exp_summary["soft_decline_retry_experiment"]["statistical_inference"]
        self.assertGreater(inf["absolute_lift_pp"], 7.0)
        self.assertGreater(inf["relative_lift_pct"], 29.0)
        self.assertLess(inf["p_value"], 1e-10)

    def test_05_qbr_scorecard_completeness(self):
        # 6 quarters * 4 merchant segments = 24 complete QBR segment-quarter rows
        self.assertEqual(len(self.scorecard), 24)
        for row in self.scorecard:
            self.assertGreater(row["gross_payment_volume_usd"], 0)
            self.assertGreaterEqual(row["overall_auth_rate_pct"], 90.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
