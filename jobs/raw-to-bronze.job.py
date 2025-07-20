from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("").getOrCreate()

table_name = "fruits.operations_bronze"

print(f"Create table {table_name}, if not exists...")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        name STRING,
        price DECIMAL(10,2),
        op_type STRING,
        op_quantity_snapshot INT,
        op_quantity_diff INT,
        op_timestamp TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (name)
""")

print(f"Transform raw to bronze...")
spark.sql(f"""
    INSERT INTO {table_name}
    SELECT
      after.name AS name,
      after.price AS price,
      CASE
        WHEN op = 'r' THEN 'warehouse'
        WHEN COALESCE(before.quantity, 0) < after.quantity THEN 'warehouse'
        ELSE 'sale'
      END AS op_type,
      after.quantity AS op_quantity_snapshot,
      ABS(after.quantity - COALESCE(before.quantity, 0)) AS op_quantity_diff,
      FROM_UTC_TIMESTAMP(TO_TIMESTAMP(source.ts_ms / 1000), 'Europe/Paris')
    FROM
      fruits.operations_raw
""")

spark.stop()
