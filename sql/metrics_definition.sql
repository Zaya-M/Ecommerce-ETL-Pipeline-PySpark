-- ============================================================
-- File: sql/metrics_definition.sql
-- Purpose: Pre-define business metrics and metadata for ads_kpi_summary.
-- Note: This demonstrates "Metrics Catalog" awareness—centralizing 
--       definitions instead of hardcoding logic in scripts.
-- ============================================================

-- Insert metric definitions (metadata only, values to be populated later)
INSERT INTO ads_kpi_summary (metric_name, metric_unit, metric_desc, calc_logic, stat_period)
VALUES
-- Atomic Metrics (Base statistics without dependencies)
('Total Orders', 'Unit', 'Total number of orders across the entire dataset',
 'COUNT(DISTINCT order_id) FROM ods_orders', 'Lifetime'),

('Total GMV', 'BRL', 'Sum of all successful payment values',
 'SUM(payment_value) FROM ods_order_payments', 'Lifetime'),

('Total Customers', 'User', 'Unique customers who have placed at least one order',
 'COUNT(DISTINCT customer_unique_id)', 'Lifetime'),

-- Derived Metrics (Calculated based on atomic metrics)
('Average Order Value (AOV)', 'BRL', 'Total GMV divided by Total Orders',
 'SUM(payment_value) / COUNT(DISTINCT order_id)', 'Lifetime'),

('Avg Delivery Days', 'Day', 'Average duration from order timestamp to actual delivery',
 'AVG(DATEDIFF(order_delivered_customer_date, order_purchase_timestamp))', 'Delivered Orders'),

('Order Completion Rate', '%', 'Percentage of orders with "delivered" status',
 'COUNT(status="delivered") / COUNT(*) * 100', 'Lifetime'),

-- Composite Metrics (Complex logic involving multiple dimensions or windows)
('30-Day Retention Rate', '%', 'Percentage of customers with 2+ orders within the last 30 days',
 'COUNT(customers with order_count >= 2) / COUNT(active_customers) * 100', 'Last 30 Days'),

('Positive Review Rate', '%', 'Percentage of orders with a review score of 4 or 5',
 'COUNT(review_score >= 4) / COUNT(has_review) * 100', 'Lifetime'),

('Payment Conversion Rate', '%', 'Ratio of successful payments to total created orders',
 'COUNT(status NOT IN ("canceled", "unavailable")) / COUNT(*) * 100', 'Lifetime');

-- Note: The actual metric_value will be computed via Python (ETL) 
--       and updated using the UPDATE statement later.