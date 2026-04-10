-- ============================================================
-- File: sql/create_all_tables.sql
-- Project: E-commerce User Behavior Data Platform
-- Description: 
--    1. Define 3-layer Data Warehouse schema in MySQL.
--    2. PySpark handles ETL; intermediate results stored as Parquet.
--    3. Final ADS layer results written back to MySQL for Metabase visualization.
-- ============================================================

CREATE DATABASE IF NOT EXISTS ecommerce_dw
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_unicode_ci;

USE ecommerce_dw;

-- ============================================================
-- ODS Layer: Operational Data Store
-- Raw data imported from CSV, maintaining original schema.
-- ============================================================

-- ODS: Master Order Table
CREATE TABLE IF NOT EXISTS ods_orders (
    order_id                VARCHAR(50) PRIMARY KEY,
    customer_id             VARCHAR(50),
    order_status            VARCHAR(30)     COMMENT 'Status: delivered, shipped, canceled, etc.',
    order_purchase_timestamp DATETIME       COMMENT 'Order timestamp',
    order_approved_at       DATETIME        COMMENT 'Payment approval timestamp',
    order_delivered_carrier_date DATETIME   COMMENT 'Carrier pickup timestamp',
    order_delivered_customer_date DATETIME  COMMENT 'Customer delivery timestamp',
    order_estimated_delivery_date DATETIME  COMMENT 'Estimated delivery date',
    -- ETL Metadata
    etl_timestamp           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB COMMENT='ODS Layer - Order Master Table';

-- ODS: Order Items Table
CREATE TABLE IF NOT EXISTS ods_order_items (
    order_id                VARCHAR(50),
    order_item_id           INT             COMMENT 'Sequential item ID within an order',
    product_id              VARCHAR(50),
    seller_id               VARCHAR(50),
    shipping_limit_date     DATETIME,
    price                   DECIMAL(10,2)   COMMENT 'Unit price (BRL)',
    freight_value           DECIMAL(10,2)   COMMENT 'Freight/Shipping cost',
    etl_timestamp           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id, order_item_id)
) ENGINE=InnoDB COMMENT='ODS Layer - Order Item Details';

-- ODS: Payment Information
CREATE TABLE IF NOT EXISTS ods_order_payments (
    order_id                VARCHAR(50),
    payment_sequential      INT             COMMENT 'Multiple payment methods may exist for one order',
    payment_type            VARCHAR(30)     COMMENT 'Type: credit_card, boleto, debit_card, etc.',
    payment_installments    INT             COMMENT 'Number of installments',
    payment_value           DECIMAL(10,2)   COMMENT 'Transaction value',
    etl_timestamp           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id, payment_sequential)
) ENGINE=InnoDB COMMENT='ODS Layer - Order Payment Details';

-- ODS: Product Information
CREATE TABLE IF NOT EXISTS ods_products (
    product_id              VARCHAR(50) PRIMARY KEY,
    product_category_name   VARCHAR(100)    COMMENT 'Category name (Portuguese)',
    product_name_length     INT,
    product_description_length INT,
    product_photos_qty      INT,
    product_weight_g        INT,
    product_length_cm       INT,
    product_height_cm       INT,
    product_width_cm        INT,
    etl_timestamp           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB COMMENT='ODS Layer - Product Master Data';

-- ODS: Customer Information
CREATE TABLE IF NOT EXISTS ods_customers (
    customer_id             VARCHAR(50) PRIMARY KEY,
    customer_unique_id      VARCHAR(50)     COMMENT 'Unique identifier for the physical customer',
    customer_zip_code_prefix VARCHAR(10),
    customer_city           VARCHAR(100),
    customer_state          VARCHAR(10)
) ENGINE=InnoDB COMMENT='ODS Layer - Customer Master Data';

-- ODS: Review Information
CREATE TABLE IF NOT EXISTS ods_order_reviews (
    review_id               VARCHAR(50) PRIMARY KEY,
    order_id                VARCHAR(50),
    review_score            TINYINT         COMMENT 'Score range: 1-5',
    review_comment_title    VARCHAR(200),
    review_comment_message  TEXT,
    review_creation_date    DATETIME,
    review_answer_timestamp DATETIME,
    etl_timestamp           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB COMMENT='ODS Layer - Order Reviews';


-- ============================================================
-- DWD Layer: Data Warehouse Detail
-- Cleansed and normalized transaction-level data.
-- ============================================================

-- DWD: Order Fact Table (Wide Table)
-- Joined orders, payments, and customers into a single record per order.
CREATE TABLE IF NOT EXISTS dwd_order_wide (
    order_id                VARCHAR(50) PRIMARY KEY,
    customer_unique_id      VARCHAR(50)     COMMENT 'De-duplicated Customer ID',
    customer_state          VARCHAR(10),
    order_status            VARCHAR(30),
    -- Date dimensions
    order_purchase_date     DATE            COMMENT 'Purchase date',
    order_purchase_datetime DATETIME,
    order_delivered_date    DATE            COMMENT 'Actual delivery date (NULL if pending)',
    -- Financial metrics (Aggregated)
    total_payment_value     DECIMAL(10,2)   COMMENT 'Total order transaction value',
    total_freight_value     DECIMAL(10,2)   COMMENT 'Total freight value for the order',
    total_item_price        DECIMAL(10,2)   COMMENT 'Total price of items (excl. freight)',
    item_count              INT             COMMENT 'Number of items in the order',
    -- Payment attributes
    payment_type            VARCHAR(30)     COMMENT 'Primary payment method',
    payment_installments    INT             COMMENT 'Installment count',
    -- Logistics metrics
    delivery_days           INT             COMMENT 'Actual lead time (Delivery - Purchase)',
    estimated_days          INT             COMMENT 'Estimated lead time',
    is_late_delivery        TINYINT(1)      COMMENT 'Flag: 1 if delayed, 0 otherwise',
    -- Data quality flags
    is_valid                TINYINT(1) DEFAULT 1 COMMENT '0 if record was filtered as dirty data',
    etl_date                DATE            NOT NULL,
    INDEX idx_customer (customer_unique_id),
    INDEX idx_order_date (order_purchase_date),
    INDEX idx_status (order_status)
) ENGINE=InnoDB COMMENT='DWD Layer - Cleansed Order Wide Table';

-- DWD: Order Item Detail Fact Table
CREATE TABLE IF NOT EXISTS dwd_order_item_wide (
    order_id                VARCHAR(50),
    order_item_id           INT,
    product_id              VARCHAR(50),
    product_category_name   VARCHAR(100),
    seller_id               VARCHAR(50),
    item_price              DECIMAL(10,2),
    freight_value           DECIMAL(10,2),
    etl_date                DATE NOT NULL,
    PRIMARY KEY (order_id, order_item_id),
    INDEX idx_product (product_id),
    INDEX idx_category (product_category_name)
) ENGINE=InnoDB COMMENT='DWD Layer - Denormalized Order Item Table';


-- ============================================================
-- DWS Layer: Data Warehouse Service
-- Aggregated metrics grouped by themes (Subject-oriented).
-- ============================================================

-- DWS: Daily GMV and Order Statistics
CREATE TABLE IF NOT EXISTS dws_daily_order_stats (
    stat_date               DATE PRIMARY KEY,
    total_orders            INT             COMMENT 'Total orders per day',
    delivered_orders        INT             COMMENT 'Number of completed orders',
    canceled_orders         INT             COMMENT 'Number of canceled orders',
    gmv                     DECIMAL(15,2)   COMMENT 'Gross Merchandise Volume',
    avg_order_value         DECIMAL(10,2)   COMMENT 'Average Transaction Value (ATV)',
    total_customers         INT             COMMENT 'Daily Unique Customers (DAU)',
    new_customers           INT             COMMENT 'First-time purchasers',
    etl_timestamp           TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB COMMENT='DWS Layer - Daily Order Aggregation';

-- DWS: Category Performance Statistics
CREATE TABLE IF NOT EXISTS dws_category_stats (
    stat_month              VARCHAR(7)      COMMENT 'Month: YYYY-MM',
    product_category_name   VARCHAR(100),
    total_orders            INT,
    total_revenue           DECIMAL(15,2),
    avg_item_price          DECIMAL(10,2),
    avg_review_score        DECIMAL(3,2),
    PRIMARY KEY (stat_month, product_category_name)
) ENGINE=InnoDB COMMENT='DWS Layer - Monthly Category Performance';


-- ============================================================
-- ADS Layer: Application Data Store
-- High-level tables optimized for BI tools and Dashboards.
-- ============================================================

-- ADS: Daily GMV Trend (for Line Charts)
CREATE TABLE IF NOT EXISTS ads_gmv_trend (
    stat_date               DATE PRIMARY KEY,
    gmv                     DECIMAL(15,2),
    order_count             INT,
    customer_count          INT,
    avg_order_value         DECIMAL(10,2),
    gmv_vs_yesterday_pct    DECIMAL(10,2)   COMMENT 'Day-over-Day (DoD) growth rate (%)'
) ENGINE=InnoDB COMMENT='ADS Layer - Daily GMV Trends';

-- ADS: Top 10 Product Categories (for Bar Charts)
CREATE TABLE IF NOT EXISTS ads_top_categories (
    rank_no                 INT PRIMARY KEY,
    product_category_name   VARCHAR(100),
    total_revenue           DECIMAL(15,2),
    total_orders            INT,
    avg_review_score        DECIMAL(3,2),
    revenue_pct             DECIMAL(5,2)    COMMENT 'Revenue Contribution (%)'
) ENGINE=InnoDB COMMENT='ADS Layer - Product Category Ranking';

-- ADS: Key Performance Indicators (KPI) Summary
CREATE TABLE IF NOT EXISTS ads_kpi_summary (
    metric_name             VARCHAR(100) PRIMARY KEY COMMENT 'KPI Name',
    metric_value            DECIMAL(20,4)            COMMENT 'KPI Value',
    metric_unit             VARCHAR(20)              COMMENT 'Unit (Count/BRL/Day/%)',
    metric_desc             VARCHAR(500)             COMMENT 'KPI Definition',
    calc_logic              VARCHAR(1000)            COMMENT 'Calculation Logic',
    stat_period             VARCHAR(50)              COMMENT 'Time Period (Full/30D/etc.)',
    etl_timestamp           TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB COMMENT='ADS Layer - Core Business KPI Summary';


-- ============================================================
-- File: sql/metrics_definition.sql
-- Description: Define business metrics for ads_kpi_summary.
-- Showcase: This demonstrates a structured Metric System approach.
-- ============================================================

INSERT INTO ads_kpi_summary (metric_name, metric_unit, metric_desc, calc_logic, stat_period)
VALUES
-- Atomic Metrics (Base measurements)
('Total Orders',         'Count',  'Total unique orders across the full dataset',
 'COUNT(DISTINCT order_id) FROM ods_orders', 'Lifetime'),

('Total GMV',           'BRL',    'Sum of all payment values for confirmed orders',
 'SUM(payment_value) FROM ods_order_payments', 'Lifetime'),

('Total Customers',      'User',   'Total unique customer IDs with at least one order',
 'COUNT(DISTINCT customer_unique_id)', 'Lifetime'),

-- Derived Metrics (Calculated from Atomic Metrics)
('Average Order Value',  'BRL',    'GMV divided by total orders',
 'SUM(payment_value) / COUNT(DISTINCT order_id)', 'Lifetime'),

('Avg Delivery Time',    'Days',   'Average lead time between purchase and delivery',
 'AVG(DATEDIFF(order_delivered_customer_date, order_purchase_timestamp))', 'Delivered Orders'),

('Completion Rate',      '%',      'Percentage of orders with status "delivered"',
 'COUNT(status=delivered) / COUNT(*) * 100', 'Lifetime'),

-- Compound Metrics (Complex logic/window-based)
('30D Repurchase Rate',  '%',      'Ratio of customers with 2+ orders within last 30 days',
 'COUNT(customers with order_count>=2) / COUNT(active_customers) * 100', 'Past 30 Days'),

('Positive Review Rate', '%',      'Percentage of orders with review score >= 4',
 'COUNT(review_score>=4) / COUNT(has_review) * 100', 'Lifetime'),

('Payment Conversion Rate', '%',   'Percentage of non-canceled/available orders vs total',
 'COUNT(status NOT IN canceled,unavailable) / COUNT(*) * 100', 'Lifetime');