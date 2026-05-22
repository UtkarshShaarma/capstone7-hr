# Databricks notebook source
# MAGIC %run ./00_setup

# COMMAND ----------

dbutils.widgets.text("run_id",   "manual_test_run")
dbutils.widgets.text("run_date", "2026-05-20")
run_id   = dbutils.widgets.get("run_id")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

from pyspark.sql import functions as F
 
# Build source path from run_date. ADF wrote files to raw/attendance/yyyy/MM/dd/attendance.csv
date_partition = run_date
src_path = f"{BASE}/raw/department/2026-05-21/department_master.csv"
print(f"Source path: {src_path}")
 
print(f"Reading from: {src_path}")
 
# Read raw CSV — header row, infer types
raw = (spark.read
       .option("header", True)
       .option("inferSchema", True)
       .csv(src_path))
 
print(f"Raw rows: {raw.count()}")
display(raw.limit(5))
 
# Add Bronze metadata: when we ingested, what file it came from, which pipeline run
bronze = (raw
    .withColumn("_ingest_ts",   F.current_timestamp())
    .withColumn("_source_file", F.expr("_metadata.file_path"))
    .withColumn("_run_id",      F.lit(run_id)))
 
print(f"Bronze columns: {bronze.columns}")

# COMMAND ----------

# Cell 3 — append to Bronze Delta table
bronze_path = f"{BASE}/bronze/department"
 
(bronze.write
       .format("delta")
       .mode("append")                       # always append — keep all history
       .option("mergeSchema", "true")        # allow new columns to appear over time
       .save(bronze_path))
 
print(f"Wrote {bronze.count()} rows to {bronze_path}")
 
# Verify by reading back
verify = spark.read.format("delta").load(bronze_path)
print(f"Total rows now in Bronze: {verify.count()}")
display(verify.groupBy("_run_id").count().orderBy("_run_id"))