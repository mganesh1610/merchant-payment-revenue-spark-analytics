-- =============================================================================
-- Curated Payment Funnel, Net Revenue Retention (NRR) & QBR/MBR Executive SQL
-- Compatible with Spark SQL / Snowflake / BigQuery / DuckDB / SQLite
-- =============================================================================

-- 1. Deduplicated Payment Settlement Fact View with Accounting Parity Guardrails
WITH deduplicated_txns AS (
    SELECT
        p.payment_txn_id,
        p.order_id,
        p.merchant_id,
        p.customer_id,
        m.merchant_segment,
        m.region_code,
        m.self_service_enabled,
        p.payment_sequential,
        p.payment_type,
        p.payment_installments,
        p.payment_value,
        p.gross_processing_fee,
        p.net_settled_value,
        p.auth_status,
        p.retry_policy_arm,
        p.retry_recovered,
        p.settlement_lag_hours,
        p.reporting_month,
        p.reporting_quarter,
        ROW_NUMBER() OVER (
            PARTITION BY p.order_id, p.payment_sequential
            ORDER BY p.payment_txn_id
        ) AS row_num
    FROM raw_order_payments p
    INNER JOIN dim_merchants m
        ON p.merchant_id = m.merchant_id
    WHERE p.payment_value > 0
),
curated_settlements AS (
    SELECT
        *,
        CASE WHEN auth_status LIKE 'AUTHORIZED%' THEN 1 ELSE 0 END AS is_authorized
    FROM deduplicated_txns
    WHERE row_num = 1
)
-- 2. Executive QBR / MBR Payment Volume, Authorization Rate & Settlement Velocity Summary
SELECT
    reporting_quarter,
    merchant_segment,
    COUNT(*) AS total_transactions,
    COUNT(DISTINCT order_id) AS unique_orders,
    COUNT(DISTINCT merchant_id) AS active_merchants,
    ROUND(SUM(payment_value), 2) AS gross_payment_volume_usd,
    ROUND(SUM(net_settled_value), 2) AS net_settled_revenue_usd,
    ROUND(100.0 * AVG(is_authorized), 2) AS overall_auth_rate_pct,
    ROUND(AVG(CASE WHEN is_authorized = 1 THEN settlement_lag_hours END), 2) AS avg_settlement_lag_hrs
FROM curated_settlements
GROUP BY reporting_quarter, merchant_segment
ORDER BY reporting_quarter, merchant_segment;
