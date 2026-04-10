# ============================================================
# File: src/spark/spark_session.py
# Purpose: Create and configure SparkSession (entry point of PySpark)
# Environment: Requires pyspark (pip install pyspark)
#              Local mode execution, no cluster required
# ============================================================

from pyspark.sql import SparkSession
import os
from src.utils.logger import get_logger

logger = get_logger("SparkSession")


def get_spark_session(app_name: str = "EcommerceDW") -> SparkSession:
    """
    Get or create a SparkSession (singleton pattern)

    Local mode:
    Uses all available CPU cores on the local machine.
    Suitable for learning and medium-scale data processing.

    Usage:
        from src.spark.spark_session import get_spark_session
        spark = get_spark_session("my_etl_job")
        df = spark.read.csv("data/raw/orders.csv")
    """

    jar_path = "/root/projects/ecommerce_dw/jars/mysql-connector-j-9.6.0.jar"

    try:
        spark = (
            SparkSession.builder.appName(app_name)
            .master("local[1]")
            .config("spark.jars", jar_path)
            .config("spark.driver.extraClassPath", jar_path)
            .config("spark.driver.memory", "512m")
            .config("spark.executor.memory", "512m")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .getOrCreate()
        )

        # Reduce Spark internal log noise for readability
        spark.sparkContext.setLogLevel("WARN")

        logger.info(f"SparkSession '{app_name}' initialized successfully")

        # NOTE:
        # You should return the same SparkSession instance created above.
        # Current code below re-builds a new session (potential bug).

        return (
            SparkSession.builder.appName(app_name)
            .config("spark.jars", jar_path)
            .config("spark.driver.extraClassPath", jar_path)
            .getOrCreate()
        )

    except Exception as e:
        logger.error(f"Failed to initialize SparkSession: {e}")
        raise e


# ============================================================
# Quick test: run this file to verify Spark environment
# Command: python src/spark/spark_session.py
# ============================================================

if __name__ == "__main__":
    try:
        spark = get_spark_session("TestSession")

        print(f"Spark Version: {spark.version}")

        test_df = spark.createDataFrame(
            [(1, "Zaya"), (2, "Data Engineer")], ["id", "name"]
        )
        test_df.show()

        logger.info("Spark execution test passed!")

    except Exception as e:
        print(f"Spark test failed: {e}")
