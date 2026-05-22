# Databricks notebook source
# MAGIC %run ./00_setup

# COMMAND ----------

dbutils.widgets.text("run_id",   "manual_test_run")
dbutils.widgets.text("run_date", "2026-05-21")
run_id   = dbutils.widgets.get("run_id")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

# Read Bronze, take latest per department_id, write Silver
from pyspark.sql import functions as F, Window
 
bronze = spark.read.format("delta").load(f"{BASE}/bronze/department")
w = Window.partitionBy("department_id").orderBy(F.desc("_ingest_ts"))
silver = (bronze
    .withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1")
    .drop("_rn"))
 
(silver.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(f"{BASE}/silver/department"))
 
print(f"Silver department: {silver.count()} rows")
display(silver)