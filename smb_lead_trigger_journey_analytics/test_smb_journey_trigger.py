#!/usr/bin/env python3
"""Unit tests for SMB Lead-Trigger & Digital Journey Funnel P1 Module."""

import unittest
from pathlib import Path

from smb_lead_trigger_journey_analytics.smb_journey_trigger_engine import (
    run_smb_journey_trigger_module,
)


class TestSMBJourneyTriggerModule(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.mod_dir = Path(__file__).resolve().parent
        cls.summary = run_smb_journey_trigger_module(cls.mod_dir)

    def test_01_merchant_and_transaction_conservation(self) -> None:
        self.assertEqual(self.summary["total_merchants_evaluated"], 3095)
        self.assertEqual(self.summary["total_payment_transactions_reconciled"], 103886)

    def test_02_governance_and_audit_controls_10_of_10(self) -> None:
        self.assertEqual(self.summary["governance_checks_passed"], 10)
        self.assertEqual(self.summary["governance_checks_total"], 10)
        for k, v in self.summary["governance_assertions"].items():
            self.assertTrue(v, f"Failed check: {k}")

    def test_03_trigger_experiment_statistical_significance(self) -> None:
        z_res = self.summary["trigger_experiment_z_test"]
        self.assertGreater(z_res["absolute_lift_pp"], 12.0)
        self.assertGreater(z_res["z_stat"], 8.0)

    def test_04_outputs_written(self) -> None:
        outputs = self.mod_dir / "outputs"
        self.assertTrue((outputs / "smb_journey_trigger_cohort_scorecard.csv").exists())
        self.assertTrue((outputs / "smb_journey_trigger_summary.json").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
