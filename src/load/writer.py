# ============================================================
# File: src/load/writer.py
# Purpose: Write PySpark-processed results from Parquet into MySQL
# Bridge between PySpark computation outputs → MySQL (for Metabase visualization)
# ============================================================

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from sqlalchemy import text
from src.utils.db_conn import DBConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)


class ParquetToMySQLWriter:
    """
    Write Parquet files (PySpark outputs) into MySQL.

    Strategy:
    - Read Parquet using PySpark
    - Convert to Pandas DataFrame
    - Write into MySQL

    Note:
    Suitable for ADS/DWS small datasets (typically < 10k rows).
    Not recommended for large-scale data due to driver memory constraints.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.engine = DBConnection.get_engine()

    def write_dws_to_mysql(self, parquet_path: str, mysql_table: str):
        """
        Load DWS Parquet data and persist it into MySQL table.

        Steps:
        1. Read Parquet with PySpark
        2. Convert Spark DataFrame → Pandas DataFrame
        3. Write into MySQL
        """

        logger.info(f"Writing data into {mysql_table}...")

        try:
            # 1. Load Parquet via Spark
            spark_df = self.spark.read.parquet(parquet_path)

            # 2. Convert to Pandas (safe for small aggregated datasets)
            pandas_df = spark_df.toPandas()
            logger.info(f"Loaded {len(pandas_df)} rows")

            # 3. Write into MySQL (overwrite strategy for idempotency)
            with self.engine.begin() as conn:
                pandas_df.to_sql(
                    name=mysql_table,
                    con=conn,
                    if_exists="replace",
                    index=False,
                    chunksize=1000,
                )

            logger.info(f"Table {mysql_table} written successfully!")

        except Exception as e:
            logger.error(f"Failed to write {mysql_table}: {e}")
            raise e

    def build_ads_top_categories(self):
        """
        Build ADS layer: Top product categories ranking from DWS layer.
        """

        sql = """
        SELECT 
            ROW_NUMBER() OVER (ORDER BY t.total_revenue DESC) AS rank_no,
            t.product_category_name,
            ROUND(t.total_revenue, 2) AS total_revenue,
            t.total_orders,
            ROUND((t.total_revenue / total_all.grand_total) * 100, 2) AS revenue_pct
        FROM (
            SELECT 
                product_category_name, 
                SUM(total_revenue) AS total_revenue,
                SUM(total_orders) AS total_orders
            FROM dws_category_stats
            GROUP BY product_category_name
        ) t
        CROSS JOIN (SELECT SUM(total_revenue) AS grand_total FROM dws_category_stats) total_all
        ORDER BY t.total_revenue DESC
        LIMIT 10
        """

        try:
            logger.info("Executing ADS top categories query...")
            ads_df = pd.read_sql(sql, self.engine)

            with self.engine.begin() as conn:
                ads_df.to_sql(
                    "ads_top_categories", conn, if_exists="replace", index=False
                )

            logger.info("ADS table ads_top_categories built successfully!")

            print("\n--- Top 3 Category Leaderboard ---")
            print(ads_df.head(3))

        except Exception as e:
            logger.error(f"Failed to build ADS top categories: {e}")

    def build_ads_gmv_trend(self):
        """
        Build ADS layer: Daily GMV trend with growth rate (MoM-like day-over-day change).
        """

        logger.info("Building ADS layer: GMV trend with growth rate...")

        sql = """
        WITH daily_data AS (
            SELECT
                stat_date,
                gmv,
                total_orders    AS order_count,
                total_customers AS customer_count
            FROM dws_daily_order_stats
        )
        SELECT
            stat_date,
            gmv,
            order_count,
            customer_count,
            ROUND(
                (gmv - LAG(gmv) OVER (ORDER BY stat_date)) / 
                NULLIF(LAG(gmv) OVER (ORDER BY stat_date), 0) * 100,
            2) AS gmv_growth_pct
        FROM daily_data
        ORDER BY stat_date DESC
        """

        try:
            ads_df = pd.read_sql(sql, self.engine)

            with self.engine.begin() as conn:
                ads_df.to_sql("ads_gmv_trend", conn, if_exists="replace", index=False)

            logger.info("ads_gmv_trend built successfully")

        except Exception as e:
            logger.error(f"Failed to build GMV trend: {e}")


if __name__ == "__main__":
    from src.spark.spark_session import get_spark_session

    spark = get_spark_session("MySQL_Writer_Job")
    writer = ParquetToMySQLWriter(spark)

    # 1. Load DWS layer into MySQL
    writer.write_dws_to_mysql("output/dws/daily_order_stats", "dws_daily_order_stats")
    writer.write_dws_to_mysql("output/dws/category_stats", "dws_category_stats")

    # 2. Build ADS layer inside MySQL
    writer.build_ads_top_categories()
    writer.build_ads_gmv_trend()


# Optional JDBC test (Spark → MySQL direct write)
# df = spark.read.parquet("output/dws/daily_order_stats")
# df.write.format("jdbc").option(
#     "url", "jdbc:mysql://localhost:3306/ecommerce_dw"
# ).option("driver", "com.mysql.cj.jdbc.Driver").option(
#     "dbtable", "dws_daily_order_stats"
# ).option(
#     "user", "root"
# ).option(
#     "password", "andrey0701"
# ).mode(
#     "append"
# ).save()
