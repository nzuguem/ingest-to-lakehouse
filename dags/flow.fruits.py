from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
}

with DAG(
        dag_id='spark_jobs_orchestration',
        default_args=default_args,
        schedule=timedelta(minutes=5),
) as dag:

    # Since the deployment mode is “client” (because the application is a Python program),
    # all configurations are provided to the Operator, who then passes them on to Spark Workers/Executors.
    # The Driver Program is run directly on the Airflow's Workers.
    spark_confs = {
        "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg.catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
        "spark.sql.catalog.iceberg.uri": "http://iceberg-rest-catalog:8181",
        "spark.sql.catalog.iceberg.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.catalog.iceberg.warehouse": "s3://warehouse/",
        "spark.sql.catalog.iceberg.s3.endpoint": "http://minio:9000",
        "spark.sql.catalog.iceberg.s3.path-style-access": "true",
        "spark.sql.defaultCatalog": "iceberg",
        "spark.sql.catalogImplementation": "in-memory",
    }
    spark_packages = 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1'

    row_to_bronze = SparkSubmitOperator(
        name='raw-to-bronze',
        task_id='raw-to-bronze',
        application='/opt/airflow/spark-jobs/raw-to-bronze.job.py',
        conn_id='spark',
        verbose=True,
        application_args=[],
        conf =spark_confs,
        packages=spark_packages,
    )

    bronze_to_silver = SparkSubmitOperator(
        name='bronze-to-silver',
        task_id='bronze-to-silver',
        application='/opt/airflow/spark-jobs/bronze-to-silver.job.py',
        conn_id='spark',
        verbose=True,
        application_args=[],
        conf =spark_confs,
        packages=spark_packages,
    )

    row_to_bronze >> bronze_to_silver
