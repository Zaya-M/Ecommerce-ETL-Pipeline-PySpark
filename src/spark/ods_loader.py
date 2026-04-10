# ============================================================
# File: src/spark/ods_loader.py
# Description: Load raw CSV data using PySpark and write to the ODS layer (Parquet format)
# Why Parquet: Columnar storage enables significantly faster queries and better compression than CSV
# ============================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    DateType,
)
import os
from src.utils.logger import get_logger

logger = get_logger(__name__)


class ODSLoader:
    """
    ODS layer loader.

    Reads CSV files from data/raw/ into Spark DataFrames,
    adds an etl_timestamp column, and writes them as Parquet files.
    """

    def __init__(self, spark: SparkSession, raw_data_path: str, output_path: str):
        self.spark = spark
        self.raw_data_path = raw_data_path  # Raw CSV directory
        self.output_path = output_path  # Parquet output directory

    def load_orders(self) -> DataFrame:
        """
        Load the orders dataset and return a typed DataFrame.

        Source file: olist_orders_dataset.csv

        Key fields:
        - order_purchase_timestamp: string → TIMESTAMP
        - Other date fields follow the same pattern
        """

        logger.info("Loading orders dataset...")

        # ---- Read CSV ----
        df = self.spark.read.csv(
            os.path.join(self.raw_data_path, "olist_orders_dataset.csv"),
            header=True,
            inferSchema=True,
        )

        # ---- Convert timestamp fields ----
        date_cols = [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ]
        for col_name in date_cols:
            df = df.withColumn(
                col_name, F.to_timestamp(F.col(col_name), "yyyy-MM-dd HH:mm:ss")
            )

        # ---- Add ETL metadata ----
        df = df.withColumn("etl_timestamp", F.current_timestamp())

        # ---- Log basic info ----
        logger.info(f"Orders row count: {df.count()}")
        df.printSchema()

        # ---- Write to Parquet ----
        df.write.mode("overwrite").parquet(os.path.join(self.output_path, "ods/orders"))

        logger.info("Orders dataset loaded successfully")
        return df

    def load_order_items(self) -> DataFrame:
        """
        Load the order items dataset.

        Note:
        - price and freight_value should be DoubleType
        """

        logger.info("Loading order items dataset...")

        df = self.spark.read.csv(
            os.path.join(self.raw_data_path, "olist_order_items_dataset.csv"),
            header=True,
            inferSchema=True,
        )

        # Ensure numeric types are correctly cast
        df = df.withColumn("price", F.col("price").cast(DoubleType())).withColumn(
            "freight_value", F.col("freight_value").cast(DoubleType())
        )

        df = df.withColumn("etl_timestamp", F.current_timestamp())

        df.write.mode("overwrite").parquet(
            os.path.join(self.output_path, "ods/order_items")
        )

        logger.info("Order items dataset loaded successfully")
        return df

    def load_payments(self) -> DataFrame:
        """Load payments dataset"""
        logger.info("Loading payments dataset...")
        df = self.spark.read.csv(
            os.path.join(self.raw_data_path, "olist_order_payments_dataset.csv"),
            header=True,
            inferSchema=True,
        )
        df = df.withColumn("etl_timestamp", F.current_timestamp())
        df.write.mode("overwrite").parquet(
            os.path.join(self.output_path, "ods/payments")
        )
        logger.info("Payments dataset loaded successfully")
        return df

    def load_products(self) -> DataFrame:
        """Load products dataset"""
        logger.info("Loading products dataset...")
        df = self.spark.read.csv(
            os.path.join(self.raw_data_path, "olist_products_dataset.csv"),
            header=True,
            inferSchema=True,
        )
        df = df.withColumn("etl_timestamp", F.current_timestamp())
        df.write.mode("overwrite").parquet(
            os.path.join(self.output_path, "ods/products")
        )
        logger.info("Products dataset loaded successfully")
        return df

    def load_customers(self) -> DataFrame:
        """Load customers dataset"""
        logger.info("Loading customers dataset...")
        df = self.spark.read.csv(
            os.path.join(self.raw_data_path, "olist_customers_dataset.csv"),
            header=True,
            inferSchema=True,
        )
        df = df.withColumn("etl_timestamp", F.current_timestamp())
        df.write.mode("overwrite").parquet(
            os.path.join(self.output_path, "ods/customers")
        )
        logger.info("Customers dataset loaded successfully")
        return df

    def load_reviews(self) -> DataFrame:
        """Load reviews dataset"""
        logger.info("Loading reviews dataset...")
        df = self.spark.read.csv(
            os.path.join(self.raw_data_path, "olist_order_reviews_dataset.csv"),
            header=True,
            inferSchema=True,
        )

        # Convert review timestamp fields
        review_date_cols = ["review_creation_date", "review_answer_timestamp"]
        for col in review_date_cols:
            df = df.withColumn(col, F.to_timestamp(F.col(col), "yyyy-MM-dd HH:mm:ss"))

        df = df.withColumn("etl_timestamp", F.current_timestamp())

        df.write.mode("overwrite").parquet(
            os.path.join(self.output_path, "ods/reviews")
        )

        logger.info("Reviews dataset loaded successfully")
        return df

    def load_all(self):
        """
        Load all datasets into the ODS layer
        """

        logger.info("Starting full ODS load...")

        self.load_orders()
        self.load_order_items()
        self.load_payments()
        self.load_products()
        self.load_customers()
        self.load_reviews()

        logger.info("ODS load completed: 6 tables processed")


if __name__ == "__main__":
    from src.spark.spark_session import get_spark_session

    spark_sess = get_spark_session("ODS_Loader_Test")

    # Initialize paths
    loader = ODSLoader(
        spark=spark_sess, raw_data_path="data/raw/", output_path="output/"
    )

    loader.load_all()
