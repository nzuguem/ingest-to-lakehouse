from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("").getOrCreate()

current_date_df = spark.sql(f"""
  SELECT FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'Europe/Paris') AS current_date_ts
""")
current_date_ts = current_date_df.first()['current_date_ts']

table_name = "fruits.operations_silver"

print(f"Create table {table_name}, if not exists...")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
      calc_timestamp TIMESTAMP,
      name STRING,
      total_sales_since_last_calc DECIMAL(20,2)
    )
    USING iceberg
    PARTITIONED BY (calc_timestamp);
""")

last_calc_df = spark.sql(f"""
  SELECT MAX(calc_timestamp) AS last_calc_ts
  FROM {table_name}
""")

last_calc_ts = last_calc_df.first()['last_calc_ts']

if last_calc_ts is None:
    condition_since_last_calc = "TRUE"
else:
    condition_since_last_calc = f"op_timestamp > TIMESTAMP '{last_calc_ts}'"

print(f"Transform bronze to silver...")
spark.sql(f"""
  WITH fruits AS (
    SELECT DISTINCT name
    FROM fruits.operations_bronze
  ),
  
  since_last_calc AS (
    SELECT
      name,
      SUM(price * op_quantity_diff) AS total_sales_since_last_calc
    FROM
      fruits.operations_bronze
    WHERE
      op_type = 'sale'
      AND {condition_since_last_calc}
    GROUP BY
      name
  )

  INSERT INTO {table_name}
  SELECT
    TIMESTAMP '{current_date_ts}' AS calc_timestamp,
    f.name,
    COALESCE(s.total_sales_since_last_calc, 0) AS total_sales_since_last_calc
  FROM
    fruits f
  LEFT JOIN
    since_last_calc s ON f.name = s.name
""")

spark.stop()
