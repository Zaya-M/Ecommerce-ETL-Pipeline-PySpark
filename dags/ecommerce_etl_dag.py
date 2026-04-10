# ============================================================
# File: dags/ecommerce_etl_dag.py
# Purpose: Airflow DAG for e-commerce data platform ETL pipeline
# ============================================================

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys

# Add project root to system path so Airflow can import src modules
PROJECT_HOME = "/root/projects/ecommerce_dw"
if PROJECT_HOME not in sys.path:
    sys.path.append(PROJECT_HOME)

default_args = {
    "owner": "zaya",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="ecommerce_etl_pipeline",
    default_args=default_args,
    description="E-commerce user behavior ETL pipeline",
    schedule_interval=None,
    catchup=False,
    tags=["ecommerce", "pyspark", "etl"],
) as dag:

    # Define data layer paths
    RAW_DATA_PATH = os.path.join(PROJECT_HOME, "data/raw")
    ODS_PATH = os.path.join(PROJECT_HOME, "output/ods")
    DWD_PATH = os.path.join(PROJECT_HOME, "output/dwd")
    DWS_PATH = os.path.join(PROJECT_HOME, "output/dws")

    # ============================================================
    # Task 1: Load raw data into ODS layer
    # ============================================================
    def load_ods_fn(**context):
        from src.spark.spark_session import get_spark_session
        from src.spark.ods_loader import ODSLoader

        spark = get_spark_session("Airflow_ODS_Loader")
        loader = ODSLoader(spark, RAW_DATA_PATH, PROJECT_HOME)
        loader.load_all()

        spark.stop()

    load_ods = PythonOperator(
        task_id="load_ods",
        python_callable=load_ods_fn,
    )

    # ============================================================
    # Task 2: Transform ODS → DWD
    # ============================================================
    def transform_dwd_fn(**context):
        from src.spark.spark_session import get_spark_session
        from src.spark.dwd_transformer import DWDTransformer

        spark = get_spark_session("Airflow_DWD_Transformer")
        transformer = DWDTransformer(spark, ODS_PATH, DWD_PATH)

        transformer.build_order_wide()
        transformer.build_order_item_wide()

        spark.stop()

    transform_dwd = PythonOperator(
        task_id="transform_dwd",
        python_callable=transform_dwd_fn,
    )

    # ============================================================
    # Task 3: Aggregate DWD → DWS
    # ============================================================
    def aggregate_dws_fn(**context):
        from src.spark.spark_session import get_spark_session
        from src.spark.dws_aggregator import DWSAggregator

        spark = get_spark_session("Airflow_DWS_Aggregator")
        aggregator = DWSAggregator(spark, DWD_PATH, DWS_PATH)

        aggregator.build_daily_order_stats()
        aggregator.build_category_stats()

        spark.stop()

    aggregate_dws = PythonOperator(
        task_id="aggregate_dws",
        python_callable=aggregate_dws_fn,
    )

    # ============================================================
    # Task 4: Load DWS data into MySQL and generate ADS layer
    # ============================================================
    def write_dws_mysql_fn(**context):
        from src.spark.spark_session import get_spark_session
        from src.load.writer import ParquetToMySQLWriter

        spark = get_spark_session("Airflow_MySQL_Writer")
        writer = ParquetToMySQLWriter(spark)

        writer.write_dws_to_mysql(
            os.path.join(DWS_PATH, "daily_order_stats"), "dws_daily_order_stats"
        )

        writer.write_dws_to_mysql(
            os.path.join(DWS_PATH, "category_stats"), "dws_category_stats"
        )

        writer.build_ads_top_categories()
        writer.build_ads_gmv_trend()

        spark.stop()

    write_dws_mysql = PythonOperator(
        task_id="write_dws_to_mysql",
        python_callable=write_dws_mysql_fn,
    )

    # ============================================================
    # Task dependencies
    # ============================================================
    load_ods >> transform_dwd >> aggregate_dws >> write_dws_mysql
