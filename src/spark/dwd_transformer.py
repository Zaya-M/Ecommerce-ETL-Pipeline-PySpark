# ============================================================
# File: src/spark/dwd_transformer.py
# Description: ODS → DWD transformation using PySpark
# Core module demonstrating data cleaning and multi-table joins
# ============================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os
from src.utils.logger import get_logger

logger = get_logger(__name__)


class DWDTransformer:
    """
    DWD layer transformer

    Reads Parquet data from the ODS layer, performs data cleaning and joins,
    and writes the transformed data into the DWD layer.
    """

    def __init__(self, spark: SparkSession, ods_path: str, dwd_path: str):
        self.spark = spark
        self.ods_path = ods_path
        self.dwd_path = dwd_path

    def build_order_wide(self) -> DataFrame:
        """
        Build order wide table (core DWD table)

        Joins:
            orders + order_payments + customers → dwd_order_wide

        Each row represents a single order with enriched attributes.
        """

        logger.info("Building dwd_order_wide...")

        # ---- Load ODS layer data ----
        orders_df = self.spark.read.parquet(os.path.join(self.ods_path, "orders"))
        payments_df = self.spark.read.parquet(
            os.path.join(self.ods_path, "payments")
        ).drop("etl_timestamp")
        customers_df = self.spark.read.parquet(
            os.path.join(self.ods_path, "customers")
        ).drop("etl_timestamp")

        # ---- Aggregate payment data (one order may have multiple payments) ----
        payments_agg = payments_df.groupBy("order_id").agg(
            F.sum("payment_value").alias("total_payment_value"),
            F.max("payment_installments").alias("max_payment_installments"),
        )

        # Extract main payment type (highest payment value per order)
        window_spec = Window.partitionBy("order_id").orderBy(F.desc("payment_value"))
        main_payment = (
            payments_df.withColumn("rank", F.row_number().over(window_spec))
            .filter(F.col("rank") == 1)
            .select("order_id", F.col("payment_type").alias("main_payment_type"))
        )

        # Merge payment aggregations
        payments_final = payments_agg.join(main_payment, "order_id", "left")

        # ---- Join customers ----
        order_wide = orders_df.join(customers_df, "customer_id", "left")

        # ---- Join payment summary ----
        order_wide = order_wide.join(payments_final, "order_id", "left")

        # ---- Derived columns ----
        # Actual delivery duration
        order_wide = order_wide.withColumn(
            "delivery_days",
            F.datediff("order_delivered_customer_date", "order_purchase_timestamp"),
        )

        # Estimated delivery duration
        order_wide = order_wide.withColumn(
            "estimated_days",
            F.datediff("order_estimated_delivery_date", "order_purchase_timestamp"),
        )

        # Delivery delay flag
        order_wide = order_wide.withColumn(
            "is_late_delivery",
            F.when(
                F.to_date("order_delivered_customer_date")
                > F.to_date("order_estimated_delivery_date"),
                1,
            )
            .when(F.col("order_delivered_customer_date").isNull(), None)
            .otherwise(0),
        )

        # Extract order date (date only)
        order_wide = order_wide.withColumn(
            "order_purchase_date", F.to_date("order_purchase_timestamp")
        )

        # ---- Data quality filtering ----
        order_wide = order_wide.filter(
            F.col("order_id").isNotNull()
            & F.col("order_purchase_timestamp").isNotNull()
        )

        order_wide = order_wide.withColumn("is_valid", F.lit(1)).withColumn(
            "etl_date", F.current_date()
        )

        # ---- Write DWD layer ----
        output_path = os.path.join(self.dwd_path, "order_wide")
        order_wide.write.mode("overwrite").partitionBy("order_purchase_date").parquet(
            output_path
        )

        logger.info(f"dwd_order_wide built successfully. Path: {output_path}")
        return order_wide

    def build_order_item_wide(self) -> DataFrame:
        """
        Build order item wide table

        Joins:
            order_items + products → dwd_order_item_wide

        Each row represents an order item enriched with product attributes.
        """

        logger.info("Building order item wide table...")

        # Load ODS data
        items_df = self.spark.read.parquet(os.path.join(self.ods_path, "order_items"))
        products_df = self.spark.read.parquet(
            os.path.join(self.ods_path, "products")
        ).drop("etl_timestamp")

        # Join and select required fields
        item_wide = items_df.join(products_df, "product_id", "left").select(
            "order_id",
            "product_id",
            "seller_id",
            "price",
            "freight_value",
            "product_category_name",
            "product_weight_g",
            F.current_timestamp().alias("etl_timestamp"),
        )

        # Write output
        output_path = os.path.join(self.dwd_path, "order_item_wide")
        item_wide.write.mode("overwrite").parquet(output_path)

        logger.info(f"Order item wide table built successfully. Path: {output_path}")
        return item_wide


if __name__ == "__main__":
    from src.spark.spark_session import get_spark_session

    spark = get_spark_session("DWD_Transformer_Job")

    transformer = DWDTransformer(
        spark=spark, ods_path="output/ods", dwd_path="output/dwd"
    )

    transformer.build_order_wide()
    transformer.build_order_item_wide()
