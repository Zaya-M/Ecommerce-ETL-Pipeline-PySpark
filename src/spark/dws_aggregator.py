# ============================================================
# File: src/spark/dws_aggregator.py
# Purpose: Aggregate data from DWD to DWS layer using PySpark SQL
# Highlights:
#   - Uses Spark SQL (registerTempView + spark.sql) for better readability
#   - Demonstrates both DataFrame API and SQL-based transformations
# ============================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import os
from src.utils.logger import get_logger

logger = get_logger(__name__)


class DWSAggregator:
    """
    DWS layer aggregator.

    Reads data from the DWD layer and performs aggregations using Spark SQL.
    """

    def __init__(self, spark: SparkSession, dwd_path: str, dws_path: str):
        self.spark = spark
        self.dwd_path = dwd_path
        self.dws_path = dws_path

    def _register_temp_views(self):
        """
        Register DWD Parquet datasets as temporary views for Spark SQL queries.
        """

        order_wide_df = self.spark.read.parquet(
            os.path.join(self.dwd_path, "order_wide")
        )
        order_wide_df.createOrReplaceTempView("dwd_order_wide")

        item_wide_df = self.spark.read.parquet(
            os.path.join(self.dwd_path, "order_item_wide")
        )
        item_wide_df.createOrReplaceTempView("dwd_order_item_wide")

        logger.info("DWD temporary views registered successfully")

    def build_daily_order_stats(self) -> DataFrame:
        """
        Build daily order statistics.

        Output: dws/daily_order_stats/
        """

        self._register_temp_views()

        sql = """
        WITH customer_first_order AS (
            SELECT 
                customer_unique_id, 
                MIN(order_purchase_date) as first_order_date
            FROM dwd_order_wide
            WHERE is_valid = 1
            GROUP BY customer_unique_id
        )
        SELECT
            t1.order_purchase_date                                   AS stat_date,
            COUNT(DISTINCT t1.order_id)                              AS total_orders,
            
            -- Order counts by status
            SUM(CASE WHEN t1.order_status = 'delivered' THEN 1 ELSE 0 END) AS delivered_orders,
            SUM(CASE WHEN t1.order_status = 'canceled' THEN 1 ELSE 0 END)  AS canceled_orders,

            ROUND(SUM(t1.total_payment_value), 2)                    AS gmv,

            -- Average order value (GMV / total_orders)
            ROUND(SUM(t1.total_payment_value) / COUNT(DISTINCT t1.order_id), 2) AS avg_order_value,

            COUNT(DISTINCT t1.customer_unique_id)                    AS total_customers,

            -- New customers: first purchase occurs on the same day
            COUNT(DISTINCT CASE WHEN t1.order_purchase_date = t2.first_order_date THEN t1.customer_unique_id END) AS new_customers

        FROM dwd_order_wide t1
        LEFT JOIN customer_first_order t2 ON t1.customer_unique_id = t2.customer_unique_id
        WHERE t1.is_valid = 1
          AND t1.order_purchase_date IS NOT NULL
        GROUP BY t1.order_purchase_date
        ORDER BY t1.order_purchase_date
        """

        df = self.spark.sql(sql)

        output_path = os.path.join(self.dws_path, "daily_order_stats")
        df.write.mode("overwrite").parquet(output_path)

        logger.info(f"Daily order statistics generated at: {output_path}")
        return df

    def build_category_stats(self) -> DataFrame:
        """
        Build monthly category-level sales statistics.

        Joins order and item-level datasets.
        """

        self._register_temp_views()

        sql = """
        SELECT
            DATE_FORMAT(ow.order_purchase_date, 'yyyy-MM') AS stat_month,
            oi.product_category_name,
            
            COUNT(DISTINCT ow.order_id)                   AS total_orders,
            ROUND(SUM(oi.price), 2)                       AS total_revenue,
            ROUND(AVG(oi.price), 2)                       AS avg_item_price
            
        FROM dwd_order_wide ow
        JOIN dwd_order_item_wide oi ON ow.order_id = oi.order_id
        WHERE ow.is_valid = 1
          AND oi.product_category_name IS NOT NULL
        GROUP BY stat_month, oi.product_category_name
        ORDER BY stat_month ASC, total_revenue DESC
        """

        df = self.spark.sql(sql)
        output_path = os.path.join(self.dws_path, "category_stats")
        df.write.mode("overwrite").parquet(output_path)

        logger.info(f"Monthly category statistics generated at: {output_path}")
        return df


if __name__ == "__main__":
    from src.spark.spark_session import get_spark_session

    spark = get_spark_session("DWS_Aggregator_Job")

    # Initialize aggregator
    agg = DWSAggregator(spark, "output/dwd", "output/dws")

    # Execute aggregation jobs
    daily_report_df = agg.build_daily_order_stats()
    category_report_df = agg.build_category_stats()

    # Persist results to MySQL
    if daily_report_df is not None:
        logger.info("Syncing DWS results to MySQL...")
        daily_report_df.write.format("jdbc").option(
            "url", "jdbc:mysql://172.26.0.1:3306/ecommerce_dw"
        ).option("dbtable", "dws_daily_sales").option("user", "root").option(
            "password", "andrey0701"
        ).option(
            "driver", "com.mysql.cj.jdbc.Driver"
        ).mode(
            "overwrite"
        ).save()
        logger.info("MySQL sync completed")
