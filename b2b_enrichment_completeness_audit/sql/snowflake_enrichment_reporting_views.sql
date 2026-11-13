-- Snowflake Reporting Views for B2B Merchant Enrichment & Payment Reconciliation
-- Built with SQL Joins & CTEs for Tableau and Power BI self-serve quality tracking.

CREATE OR REPLACE VIEW analytics_prod.vw_merchant_enrichment_sla AS
WITH merchant_catalog AS (
    SELECT
        s.seller_id,
        s.seller_state,
        COUNT(oi.order_item_id) AS line_items_sold,
        SUM(CASE WHEN p.product_category_name IS NOT NULL AND p.product_weight_g IS NOT NULL THEN 1 ELSE 0 END) AS enriched_items,
        SUM(oi.price + oi.freight_value) AS gross_item_revenue
    FROM raw_commerce.sellers s
    LEFT JOIN raw_commerce.order_items oi ON s.seller_id = oi.seller_id
    LEFT JOIN raw_commerce.products p ON oi.product_id = p.product_id
    GROUP BY 1, 2
),
order_payment_check AS (
    SELECT
        o.order_id,
        COALESCE(SUM(op.payment_value), 0) AS payment_total,
        COUNT(op.payment_sequential) AS payment_rows
    FROM raw_commerce.orders o
    LEFT JOIN raw_commerce.order_payments op ON o.order_id = op.order_id
    GROUP BY 1
)
SELECT
    mc.seller_state,
    COUNT(DISTINCT mc.seller_id) AS merchant_count,
    SUM(mc.line_items_sold) AS total_line_items,
    SUM(mc.enriched_items) AS enriched_line_items,
    ROUND((SUM(mc.enriched_items) * 100.0) / NULLIF(SUM(mc.line_items_sold), 0), 2) AS catalog_enrichment_pct,
    ROUND(SUM(mc.gross_item_revenue), 2) AS gross_item_revenue,
    (SELECT COUNT(*) FROM order_payment_check WHERE payment_rows = 0) AS orders_missing_payment
FROM merchant_catalog mc
GROUP BY 1;
