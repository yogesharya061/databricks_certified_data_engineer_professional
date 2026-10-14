# Databricks notebook source

# COMMAND ----------

# MAGIC %md

# MAGIC # Video 1 — Your first governed retail lakehouse

# MAGIC Yogesshh Arya | Data Engineering • Cloud • AI

# MAGIC 

# MAGIC Run cells in order on serverless notebook compute. This is a tiny, repeatable teaching lab, not a production ingestion design.

# MAGIC Use a dedicated course catalog or a permitted existing catalog with these isolated `v01_*` schemas.

# MAGIC All writes replace only this lab's files/tables. Never point it at production schemas.

# MAGIC External-table setup is a concept demonstration, not a required cell.

# COMMAND ----------

# MAGIC %md

# MAGIC ## Cell 01 — Configuration and current context

# MAGIC Default: create a dedicated catalog in a workspace that supports default/managed storage.

# MAGIC If you created the catalog in Catalog Explorer, set CREATE_CATALOG=False.

# MAGIC If you must use an existing catalog, change CATALOG and set CREATE_CATALOG=False.

# MAGIC Do not guess an existing catalog name: select one from `SHOW CATALOGS` where you have permission.

# COMMAND ----------

import re
from pyspark.sql import functions as F

CATALOG = "retail_de_course"
CREATE_CATALOG = True
assert re.fullmatch(r"[a-z][a-z0-9_]*", CATALOG), "Use a simple lower-case catalog identifier."
BRONZE, SILVER, GOLD = "v01_bronze", "v01_silver", "v01_gold"
def table(schema, name):
    return f"`{CATALOG}`.`{schema}`.`{name}`"

display(spark.sql("SELECT current_user() AS user, current_catalog() AS catalog, current_schema() AS schema"))
display(spark.sql("SHOW CATALOGS"))
print("Lab catalog:", CATALOG)


# COMMAND ----------

# MAGIC %md

# MAGIC ## Cell 02 — Create the catalog and schemas

# MAGIC If catalog creation reports missing storage, use Catalog Explorer → Create catalog → Use default storage where offered.

# MAGIC In an enterprise workspace, use an administrator-provisioned catalog instead. Do not add a made-up MANAGED LOCATION.

# COMMAND ----------

if CREATE_CATALOG:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}` COMMENT 'Dedicated synthetic retail training lab'")
for schema, description in [
    (BRONZE, "Video 1 raw records and landing files"),
    (SILVER, "Video 1 validated records and quarantine"),
    (GOLD, "Video 1 reporting outputs"),
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{schema}` COMMENT '{description}'")
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{BRONZE}`")
display(spark.sql(f"SHOW SCHEMAS IN `{CATALOG}`"))


# COMMAND ----------

# MAGIC %md

# MAGIC ## Cell 03 — Create a managed volume

# MAGIC A volume holds files. A registered table is a separate object; table data will not be registered inside this volume.

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{BRONZE}`.`landing`")
LANDING = f"/Volumes/{CATALOG}/{BRONZE}/landing"
INPUT_DIR = f"{LANDING}/video01/input"
dbutils.fs.mkdirs(INPUT_DIR)
print("File landing directory:", INPUT_DIR)
display(spark.sql(f"SHOW VOLUMES IN `{CATALOG}`.`{BRONZE}`"))


# COMMAND ----------

# MAGIC %md

# MAGIC ## Cell 04 — Generate deterministic synthetic source data

# MAGIC 10 customers, 5 products, 20 distinct valid orders, 1 exact duplicate and 2 invalid orders.

# MAGIC The source deliberately uses strings for order fields. Only synthetic data is used.

# MAGIC No Faker installation, external download or cloud credentials are required.

# COMMAND ----------

import json
from datetime import date, timedelta

customers = [
    {"customer_id": f"C{i:03d}", "customer_name": f"Demo Customer {i:02d}",
     "city": ["Delhi", "Pune", "Bengaluru"][(i-1) % 3]}
    for i in range(1, 11)
]
products = [
    {"product_id": f"P{i:03d}", "product_name": f"Demo Product {i:02d}",
     "category": "Accessories", "unit_price": str(i * 100)}
    for i in range(1, 6)
]
orders = []
for i in range(1, 21):
    product_no = (i - 1) % 5 + 1
    orders.append({
        "order_id": f"O{i:04d}",
        "customer_id": f"C{(i-1) % 10 + 1:03d}",
        "product_id": f"P{product_no:03d}",
        "order_date": str(date(2026, 9, 1) + timedelta(days=(i-1)//10)),
        "quantity": str((i - 1) % 3 + 1),
        "unit_price": str(product_no * 100),
        "status": "CANCELLED" if i % 5 == 0 else "COMPLETED",
    })
orders.append(dict(orders[0]))
orders.append({**orders[1], "order_id": "O9001", "quantity": "-2"})
orders.append({**orders[2], "order_id": "O9002", "customer_id": None})

for name, records in [("customers", customers), ("products", products), ("orders", orders)]:
    path = f"{INPUT_DIR}/{name}.json"
    dbutils.fs.put(path, "\n".join(json.dumps(r) for r in records) + "\n", overwrite=True)
    print(name, len(records), path)
display(dbutils.fs.ls(INPUT_DIR))
print(dbutils.fs.head(f"{INPUT_DIR}/orders.json", 700))


# COMMAND ----------

# MAGIC %md

# MAGIC ## Cell 05 — Read files with explicit schemas

# MAGIC An explicit STRING landing schema makes the data-quality exercise visible and repeatable.

# MAGIC FAILFAST here detects malformed JSON structure; business-invalid values remain available for Silver validation.

# COMMAND ----------

source_schemas = {
    "customers": "customer_id STRING, customer_name STRING, city STRING",
    "products": "product_id STRING, product_name STRING, category STRING, unit_price STRING",
    "orders": "order_id STRING, customer_id STRING, product_id STRING, order_date STRING, quantity STRING, unit_price STRING, status STRING",
}
raw_frames = {
    name: spark.read.schema(schema).option("mode", "FAILFAST").json(f"{INPUT_DIR}/{name}.json")
    for name, schema in source_schemas.items()
}
display(raw_frames["orders"].orderBy("order_id"))
raw_frames["orders"].printSchema()


# COMMAND ----------

# MAGIC %md

# MAGIC ## Cell 06 — Persist Bronze as managed Delta tables

# MAGIC No LOCATION or path write option: the catalog's managed storage owns the table layout.

# MAGIC Overwrite is deliberate for this fixed demonstration snapshot. It is not a production incremental-ingestion strategy.

# COMMAND ----------

for name, frame in raw_frames.items():
    bronze_df = (frame
        .withColumn("_source_file", F.lit(f"{INPUT_DIR}/{name}.json"))
        .withColumn("_ingested_at", F.current_timestamp()))
    (bronze_df.write.format("delta").mode("overwrite")
        .saveAsTable(table(BRONZE, name)))
display(spark.sql(f"SHOW TABLES IN `{CATALOG}`.`{BRONZE}`"))
display(spark.sql(f"DESCRIBE TABLE EXTENDED {table(BRONZE, 'orders')}"))
assert spark.table(table(BRONZE, "orders")).count() == 23


# COMMAND ----------

# MAGIC %md

# MAGIC ## Cell 07 — Create the clean reference tables

# MAGIC Customer and product inputs are controlled fixtures. We assert uniqueness before joining them to orders.

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TABLE {table(SILVER, 'customers')} USING DELTA AS
SELECT customer_id, customer_name, city FROM {table(BRONZE, 'customers')}""")
spark.sql(f"""CREATE OR REPLACE TABLE {table(SILVER, 'products')} USING DELTA AS
SELECT product_id, product_name, category, CAST(unit_price AS DECIMAL(12,2)) AS unit_price
FROM {table(BRONZE, 'products')}""")
for name, key, expected in [("customers", "customer_id", 10), ("products", "product_id", 5)]:
    frame = spark.table(table(SILVER, name))
    assert frame.count() == expected
    assert frame.select(key).distinct().count() == expected
    assert frame.filter(F.col(key).isNull()).count() == 0
print("Reference table checks passed")


# COMMAND ----------

# MAGIC %md

# MAGIC ## Cell 08 — Validate, quarantine and deduplicate orders

# MAGIC TRY_CAST keeps malformed values from crashing the validation query. COALESCE makes NULL validation results invalid.

# MAGIC Quarantine keeps the raw fields and a reason. Exact duplicates are removed across business fields only.

# MAGIC This is NOT latest-record CDC logic: the fixture contains no conflicting versions for one order ID.

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TEMP VIEW v01_orders_checked AS
WITH typed AS (
  SELECT *,
    TRY_CAST(order_date AS DATE) AS order_date_typed,
    TRY_CAST(quantity AS INT) AS quantity_typed,
    TRY_CAST(unit_price AS DECIMAL(12,2)) AS unit_price_typed
  FROM {table(BRONZE, 'orders')}
)
SELECT *, COALESCE(
  order_id IS NOT NULL AND customer_id IS NOT NULL AND product_id IS NOT NULL
  AND order_date_typed IS NOT NULL AND quantity_typed > 0 AND unit_price_typed > 0
  AND status IN ('COMPLETED', 'CANCELLED'), FALSE) AS is_valid
FROM typed""")
spark.sql(f"""CREATE OR REPLACE TABLE {table(SILVER, 'orders_quarantine')} USING DELTA AS
SELECT *, CASE
  WHEN customer_id IS NULL THEN 'MISSING_CUSTOMER'
  WHEN quantity_typed IS NULL OR quantity_typed <= 0 THEN 'INVALID_QUANTITY'
  ELSE 'OTHER_INVALID_FIELD' END AS rejection_reason
FROM v01_orders_checked WHERE NOT is_valid""")
spark.sql(f"""CREATE OR REPLACE TABLE {table(SILVER, 'orders')} USING DELTA AS
SELECT DISTINCT order_id, customer_id, product_id,
  order_date_typed AS order_date, quantity_typed AS quantity,
  unit_price_typed AS unit_price, status,
  CAST(quantity_typed * unit_price_typed AS DECIMAL(14,2)) AS order_amount
FROM v01_orders_checked WHERE is_valid""")
display(spark.table(table(SILVER, "orders_quarantine")).select("order_id", "quantity", "customer_id", "rejection_reason"))
assert spark.table(table(SILVER, "orders")).count() == 20
assert spark.table(table(SILVER, "orders_quarantine")).count() == 2
assert spark.table(table(SILVER, "orders")).select("order_id").distinct().count() == 20


# COMMAND ----------

# MAGIC %md

# MAGIC ## Cell 09 — Publish business-ready Gold tables

# MAGIC For this teaching model, completed order value = quantity × unit_price, excluding cancelled orders.

# MAGIC It is not payment settlement or accounting revenue: taxes, returns, discounts and payments are not modelled yet.

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TABLE {table(GOLD, 'daily_sales')} USING DELTA AS
SELECT order_date, COUNT(*) AS completed_orders,
       SUM(order_amount) AS completed_order_value
FROM {table(SILVER, 'orders')}
WHERE status = 'COMPLETED'
GROUP BY order_date""")
spark.sql(f"""CREATE OR REPLACE TABLE {table(GOLD, 'customer_sales')} USING DELTA AS
SELECT c.customer_id, c.customer_name, c.city,
       COUNT(*) AS completed_orders, SUM(o.order_amount) AS completed_order_value
FROM {table(SILVER, 'orders')} o
JOIN {table(SILVER, 'customers')} c ON o.customer_id = c.customer_id
WHERE o.status = 'COMPLETED'
GROUP BY c.customer_id, c.customer_name, c.city""")
display(spark.table(table(GOLD, "daily_sales")).orderBy("order_date"))
display(spark.table(table(GOLD, "customer_sales")).orderBy("customer_id"))


# COMMAND ----------

# MAGIC %md

# MAGIC ## Cell 10 — Add useful metadata and inspect the objects

# MAGIC Show the same objects in Catalog Explorer after running this cell.

# COMMAND ----------

spark.sql(f"COMMENT ON TABLE {table(BRONZE, 'orders')} IS 'Video 1 synthetic source orders: duplicates and invalid values retained'")
spark.sql(f"COMMENT ON TABLE {table(SILVER, 'orders')} IS 'Video 1 valid distinct synthetic orders; includes cancelled orders'")
spark.sql(f"COMMENT ON TABLE {table(GOLD, 'daily_sales')} IS 'Completed synthetic order value by date, excluding cancellations; not settled revenue'")
display(spark.sql(f"DESCRIBE DETAIL {table(GOLD, 'daily_sales')}"))
print(f"SQL editor query: SELECT * FROM `{CATALOG}`.`{GOLD}`.`daily_sales` ORDER BY order_date;")


# COMMAND ----------

# MAGIC %md

# MAGIC ## Cell 11 — Reconcile and verify

# MAGIC Expected: Bronze 23; rejected 2; extra exact duplicate 1; Silver 20; completed 16; cancelled 4.

# COMMAND ----------

from decimal import Decimal
bronze_count = spark.table(table(BRONZE, "orders")).count()
silver_count = spark.table(table(SILVER, "orders")).count()
quarantine_count = spark.table(table(SILVER, "orders_quarantine")).count()
valid_before_dedup = spark.sql("SELECT COUNT(*) AS n FROM v01_orders_checked WHERE is_valid").first()["n"]
duplicates_removed = valid_before_dedup - silver_count
assert (bronze_count, silver_count, quarantine_count, duplicates_removed) == (23,20,2,1)
assert bronze_count == silver_count + quarantine_count + duplicates_removed
expected_value = sum(Decimal(r["quantity"]) * Decimal(r["unit_price"])
                     for r in orders[:20] if r["status"] == "COMPLETED")
result = spark.sql(f"SELECT SUM(completed_orders) AS n, SUM(completed_order_value) AS value FROM {table(GOLD, 'daily_sales')}").first()
assert result["n"] == 16
assert result["value"] == expected_value
customer_total = spark.sql(f"SELECT SUM(completed_order_value) AS value FROM {table(GOLD, 'customer_sales')}").first()["value"]
assert customer_total == expected_value
print({"bronze":bronze_count, "silver":silver_count, "quarantine":quarantine_count,
       "duplicate_rows_removed":duplicates_removed, "completed_orders":result["n"],
       "completed_order_value":str(result["value"]), "validation":"PASS"})


# COMMAND ----------

# MAGIC %md

# MAGIC ## Cell 12 — Failure demo: wrong schema (OFF by default)

# MAGIC Set RUN_NAMESPACE_FAILURE=True, run just this cell, discuss the expected error, then run Cell 13.

# MAGIC Do not Run All with demo flags enabled.

# COMMAND ----------

RUN_NAMESPACE_FAILURE = False
if RUN_NAMESPACE_FAILURE:
    spark.sql(f"USE CATALOG `{CATALOG}`")
    spark.sql(f"USE SCHEMA `{BRONZE}`")
    display(spark.sql("SELECT * FROM daily_sales"))
else:
    print("Failure demo skipped. Enable only during the troubleshooting chapter.")


# COMMAND ----------

# MAGIC %md

# MAGIC ## Cell 13 — Repair: qualify the object name

# MAGIC The table exists in Gold, not Bronze. No table recreation or permission widening is needed.

# COMMAND ----------

display(spark.sql("SELECT current_catalog(), current_schema()"))
display(spark.sql(f"SELECT * FROM {table(GOLD, 'daily_sales')} ORDER BY order_date"))


# COMMAND ----------

# MAGIC %md

# MAGIC ## Cell 14 — Failure demo: wrong volume path (OFF by default)

# MAGIC Enable just this cell. Expected: path/volume not found, not a missing-table error.

# COMMAND ----------

RUN_PATH_FAILURE = False
if RUN_PATH_FAILURE:
    dbutils.fs.ls(f"/Volumes/{CATALOG}/{BRONZE}/landing_typo/video01/input")
else:
    print("Path failure demo skipped.")


# COMMAND ----------

# MAGIC %md

# MAGIC ## Cell 15 — Repair: inspect the registered volume and exact path

# COMMAND ----------

display(spark.sql(f"SHOW VOLUMES IN `{CATALOG}`.`{BRONZE}`"))
display(dbutils.fs.ls(INPUT_DIR))


# COMMAND ----------

# MAGIC %md

# MAGIC ## Cell 16 — Failure demo: syntactically valid, incorrect metric (OFF by default)

# MAGIC This deliberately includes cancelled orders. The assertion should fail: 20 is not 16.

# MAGIC Then run Cell 17 to repair the business rule. No data is mutated by either query.

# COMMAND ----------

RUN_METRIC_FAILURE = False
if RUN_METRIC_FAILURE:
    wrong = spark.sql(f"SELECT COUNT(*) AS n, SUM(order_amount) AS value FROM {table(SILVER, 'orders')}").first()
    print("Incorrect all-status metric:", wrong.asDict())
    assert wrong["n"] == 16, "Business rule failed: cancelled orders must be excluded."
else:
    print("Metric failure demo skipped.")


# COMMAND ----------

# MAGIC %md

# MAGIC ## Cell 17 — Repair: apply the metric definition

# COMMAND ----------

display(spark.sql(f"""SELECT COUNT(*) AS completed_orders, SUM(order_amount) AS completed_order_value
FROM {table(SILVER, 'orders')} WHERE status = 'COMPLETED'"""))
print("Re-run Cell 11 to confirm complete reconciliation.")


# COMMAND ----------

# MAGIC %md

# MAGIC ## End of lesson

# MAGIC Keep these objects for the next lesson. No cleanup runs automatically.

# MAGIC To repeat the demo, reset all failure flags to False and run Cells 01–11.

# MAGIC Business outputs repeat; ingestion timestamps and Delta versions will change.

# MAGIC This lab has not implemented production append history, incremental loads, CDC, scheduling or access-policy tests.
