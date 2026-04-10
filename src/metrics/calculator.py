# ============================================================
# File: src/metrics/calculator.py
# Purpose: Compute business KPIs and update ads_kpi_summary table
# This is the most business-critical component of the project
# and a key highlight for interviews.
# ============================================================

import pandas as pd
from sqlalchemy import text
from src.utils.db_conn import DBConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)


class MetricsCalculator:
    """
    Business metrics calculator

    Reads aggregated data from the MySQL DWS layer,
    computes KPIs, and writes results into the ADS layer.

    Note:
    DWS data is pre-processed by PySpark and persisted into MySQL
    via writer.py.
    """

    def __init__(self):
        self.engine = DBConnection.get_engine()

    def _execute_scalar_sql(self, sql: str) -> float:
        """Execute SQL and return the first row, first column as float."""
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(text(sql), conn)
                if not df.empty:
                    val = df.iloc[0, 0]
                    return float(val) if val is not None else 0.0
                return 0.0
        except Exception as e:
            logger.error(f"Failed to execute metric SQL: {e}")
            return 0.0

    def calc_total_gmv(self) -> float:
        """
        Total GMV (Gross Merchandise Value)
        Definition: Sum of all paid order amounts
        """

        sql = """
        SELECT ROUND(SUM(gmv), 2) AS total_gmv
        FROM dws_daily_order_stats
        WHERE stat_date IS NOT NULL
        """

        return self._execute_scalar_sql(sql)

    def calc_avg_order_value(self) -> float:
        """
        Average Order Value (AOV)
        Definition: Total GMV / Total number of orders
        """

        sql = """
        SELECT
            ROUND(SUM(gmv) / SUM(total_orders), 2) AS avg_order_value
        FROM dws_daily_order_stats
        WHERE total_orders > 0
        """

        return self._execute_scalar_sql(sql)

    def calc_order_completion_rate(self) -> float:
        """
        Order Completion Rate
        Definition: Delivered orders / Total orders * 100
        """

        sql = """
        SELECT
            ROUND(SUM(delivered_orders) / SUM(total_orders) * 100, 2) AS completion_rate
        FROM dws_daily_order_stats
        WHERE total_orders > 0
        """

        return self._execute_scalar_sql(sql)

    def calc_avg_delivery_days(self) -> float:
        """
        Average Delivery Time (days)

        Directly computed from dwd_order_wide table,
        since this metric is not available in DWS layer.
        """

        sql = """
        SELECT ROUND(AVG(delivery_days), 1) AS avg_delivery_days
        FROM dwd_order_wide
        WHERE delivery_days IS NOT NULL
          AND delivery_days > 0
          AND delivery_days < 100
        """

        return self._execute_scalar_sql(sql)

    def calc_repurchase_rate(self) -> float:
        """
        Repurchase Rate (high-value business metric)

        Definition:
        Customers with 2+ orders / Total customers * 100
        """

        sql = """
        WITH customer_order_counts AS (
            SELECT customer_unique_id,
                   COUNT(DISTINCT order_id) AS order_count
            FROM dwd_order_wide
            WHERE is_valid = 1
            GROUP BY customer_unique_id
        )
        SELECT
            ROUND(
                SUM(CASE WHEN order_count >= 2 THEN 1 ELSE 0 END) /
                COUNT(*) * 100,
            2) AS repurchase_rate
        FROM customer_order_counts
        """

        return self._execute_scalar_sql(sql)

    def calc_late_delivery_rate(self) -> float:
        """
        Late Delivery Rate
        Definition: Orders delivered later than expected / Total valid orders
        """

        sql = """
        SELECT
            ROUND(
                SUM(CASE WHEN is_late_delivery = 1 THEN 1 ELSE 0 END) /
                COUNT(CASE WHEN is_late_delivery IS NOT NULL THEN 1 END) * 100
            , 2) AS late_delivery_rate
        FROM dwd_order_wide
        WHERE is_valid = 1
        """

        return self._execute_scalar_sql(sql)

    def update_all_metrics(self):
        """
        Compute all KPIs and update ads_kpi_summary table in batch
        """

        logger.info("Starting KPI computation and update...")

        metrics = {
            "Total GMV": self.calc_total_gmv(),
            "Average Order Value": self.calc_avg_order_value(),
            "Order Completion Rate": self.calc_order_completion_rate(),
            "Average Delivery Days": self.calc_avg_delivery_days(),
            "30-day Repurchase Rate": self.calc_repurchase_rate(),
            "Late Delivery Rate": self.calc_late_delivery_rate(),
        }

        with self.engine.begin() as conn:
            for name, value in metrics.items():
                if value is not None:
                    conn.execute(
                        text(
                            "UPDATE ads_kpi_summary SET metric_value = :v WHERE metric_name = :n"
                        ),
                        {"v": value, "n": name},
                    )
                    logger.info(f"{name}: {value}")

        logger.info(f"KPI update completed. Total metrics updated: {len(metrics)}")
