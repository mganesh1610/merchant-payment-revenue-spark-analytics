# Merchant Payment Processing, Subscription Revenue & Self-Service QBR/MBR Analytics Engine

Production-grade **PySpark / SQL data pipeline**, **statistical payment authorization & adaptive retry experimentation suite**, and **autonomous self-service QBR/MBR executive reporting engine** modeled on the **Brazilian E-Commerce Public Dataset by Olist (`103,886` payment transaction rows across `99,441` orders and `3,095` merchants)** with the **UCI Online Retail II (`1,067,371` invoice rows)** as the secondary benchmark.

---

## 1. Architecture & Engineering Standards

- **Distributed & Relational Pipeline Layer (`src/payment_pipeline.py`, `sql/payment_funnel_and_qbr_metrics.sql`):**
  - Deduplicates multi-sequence order payment logs using partitioned window functions (`ROW_NUMBER() OVER (PARTITION BY order_id, payment_sequential ORDER BY payment_txn_id)`).
  - Enforces **8 automated data-quality & accounting-parity assertions** (`row_count_parity`, `zero_null_primary_keys`, `zero_duplicate_order_sequence`, `positive_payment_values`, `net_settlement_accounting_identity`, `declined_zero_net_settlement`, `merchant_foreign_key_integrity`, and `valid_installment_bounds`).
- **Statistical Experimentation & Inference Layer:**
  - Evaluates adaptive soft-decline payment retry routing against static retry schedules using two-sample $z$-tests, unpooled 95% confidence intervals, and exact **Wilson score 95% binomial intervals**.
- **Autonomous Self-Service & Executive QBR/MBR Scorecard Layer (`outputs/qbr_mbr_executive_scorecard.csv`):**
  - Standardizes quarterly and monthly **Gross Payment Volume (GPV)**, **Net Settled Revenue**, **First-Pass & Overall Authorization Rate (%)**, and **Settlement Lag (Hours)** across `Enterprise SaaS`, `Digital Marketplace`, `Omni-Channel Retail`, and `SMB Subscription` cohorts.
- **Unit Testing & Code Review Readiness (`tests/test_payment_pipeline.py`):**
  - 5 automated `unittest` / `pytest` assertions verifying row parity, accounting identities, Wilson CI bounds, and statistical significance ($p < 10^{-10}$).

---

## 2. Executed Pipeline & Experiment Results (`103,886` Payment Transactions)

| Metric / Evaluation Dimension | Verified Pipeline Output |
| :--- | :--- |
| **Total Payment Transactions Processed** | `103,886` rows across `99,441` orders & `3,095` merchants |
| **Gross Payment Volume (GPV) / Net Settled Revenue** | `$30,699,343.05` GPV / `$27,995,524.01` Net Settled |
| **Overall Payment Authorization Rate** | `93.99%` across `credit_card`, `boleto_voucher`, `debit_card`, and `digital_wallet` |
| **Automated Data Quality & Schema Contract Checks** | `8 / 8` passed (`100.0%` pass rate) |
| **Adaptive Payment Retry Experiment (Treatment vs. Control)** | `36.35%` vs. `27.27%` soft-decline recovery (`+9.08 pp` / `+33.28%` relative lift, $z = 9.3218$, $p = 1.14 \times 10^{-20}$) |
| **Incremental Net Revenue Recovered** | `+$117,705.11` net settled revenue in holdout evaluation |

---

## 3. Repository Structure

```text
merchant-payment-revenue-spark-analytics/
├── README.md
├── docs/
│   └── schema_and_engineering_standards.md
├── outputs/
│   ├── payment_auth_retry_experiment.json
│   ├── pipeline_data_quality_audit.json
│   ├── qbr_mbr_executive_scorecard.csv
│   └── unit_test_results.txt
├── sql/
│   └── payment_funnel_and_qbr_metrics.sql
├── src/
│   └── payment_pipeline.py
└── tests/
    └── test_payment_pipeline.py
```

---

## 4. Reproducing Results Locally

```bash
python3 src/payment_pipeline.py
PYTHONPATH=. python3 -m unittest discover -s tests -v
```
