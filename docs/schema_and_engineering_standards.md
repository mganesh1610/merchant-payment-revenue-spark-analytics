# Schema Contract, Statistical Formulations & Self-Service Governance

## 1. Public Dataset Alignment & Table Schemas

1. **`dim_merchants` (`3,095` merchants):**
   - `merchant_id` (`TEXT PRIMARY KEY`), `merchant_segment` (`Enterprise SaaS`, `Digital Marketplace`, `Omni-Channel Retail`, `SMB Subscription`), `region_code`, `onboarding_quarter`, `self_service_enabled` (`INTEGER`).
2. **`raw_order_payments` & `fact_payment_settlements` (`103,886` payment rows across `99,441` orders):**
   - Aligned with Olist `olist_order_payments_dataset` (`order_id`, `payment_sequential`, `payment_type`, `payment_installments`, `payment_value`) enriched with merchant settlement fields (`gross_processing_fee`, `net_settled_value`, `auth_status`, `retry_policy_arm`, `retry_recovered`, `settlement_lag_hours`, `reporting_month`, `reporting_quarter`).

## 2. Mathematical & Statistical Formulations

- **Net Settled Revenue Accounting Identity:**
  $$\text{Net Settled Value}_i = \mathbb{I}(\text{Authorized}_i = 1) \cdot (\text{Payment Value}_i - \text{Gross Processing Fee}_i)$$
- **Wilson Score 95% Confidence Interval for Authorization & Recovery Rates:**
  $$\frac{\hat{p} + \frac{z^2}{2n} \pm z\sqrt{\frac{\hat{p}(1-\hat{p})}{n} + \frac{z^2}{4n^2}}}{1 + \frac{z^2}{n}}, \quad z = 1.959964$$
- **Two-Proportion $z$-Test for Adaptive Payment Retry Routing:**
  $$z = \frac{\hat{p}_T - \hat{p}_C}{\sqrt{\hat{p}_{\text{pool}}(1 - \hat{p}_{\text{pool}})\left(\frac{1}{n_T} + \frac{1}{n_C}\right)}}$$
