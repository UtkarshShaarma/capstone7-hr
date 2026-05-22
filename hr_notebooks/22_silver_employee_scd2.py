# Databricks notebook source
# MAGIC %run ./00_setup

# COMMAND ----------

dbutils.widgets.text("run_id",   "manual_test_run")
dbutils.widgets.text("run_date", "2026-05-21")
run_id   = dbutils.widgets.get("run_id")
run_date = dbutils.widgets.get("run_date")


# COMMAND ----------

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable
 
bronze = spark.read.format("delta").load(f"{BASE}/bronze/employee")
w = Window.partitionBy("employee_id").orderBy(F.desc("_ingest_ts"))
src = (bronze
    .withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1")
    .drop("_rn"))
 
# Type-cast
src_typed = (src
    .withColumn("hire_date", F.to_date("hire_date"))
    .withColumn("salary",    F.col("salary").cast("decimal(18,2)"))
    # SCD2 columns
    .withColumn("effective_from", F.current_timestamp())
    .withColumn("effective_to",   F.lit(None).cast("timestamp"))
    .withColumn("is_current",     F.lit(True)))
 
print(f"Source snapshot: {src_typed.count()} employees")

# COMMAND ----------

silver_path = f"{BASE}/silver/employee"
 
# First-time? Just write everything as the initial version.
if not DeltaTable.isDeltaTable(spark, silver_path):
    print("First-time init: writing all rows to Silver")
    (src_typed.write.format("delta")
              .mode("overwrite")
              .option("overwriteSchema", "true")
              .save(silver_path))
    print(f"Init complete: {src_typed.count()} rows")
else:
    print("Existing Silver detected — running SCD2 merge")
    tgt = DeltaTable.forPath(spark, silver_path)
    current_rows = tgt.toDF().filter("is_current = true").alias("c")
    incoming = src_typed.alias("s")
 
    # Identify employees whose tracked attributes have changed
    tracked_cols = ["department_id", "designation", "manager_id", "salary", "employment_status"]
    change_predicate = " OR ".join([f"c.{c} <> s.{c}" for c in tracked_cols])
 
    changed_keys = (current_rows
        .join(incoming, "employee_id", "inner")
        .filter(change_predicate)
        .select("employee_id"))
    print(f"Employees with changes: {changed_keys.count()}")
 
    # Step 1: expire (close) the current rows for changed employees
    (tgt.alias("t")
       .merge(changed_keys.alias("k"),
              "t.employee_id = k.employee_id AND t.is_current = true")
       .whenMatchedUpdate(set={
           "is_current":   "false",
           "effective_to": "current_timestamp()"
       })
       .execute())
 
    # Step 2: insert new rows for changed employees + brand-new employees
    existing_emp_ids = current_rows.select("employee_id").distinct()
    brand_new = incoming.join(existing_emp_ids, "employee_id", "left_anti")
    updated   = incoming.join(changed_keys, "employee_id", "inner").select(incoming["*"])
    to_insert = brand_new.unionByName(updated)
 
    print(f"Brand new employees: {brand_new.count()}")
    print(f"Updated employees (new current row): {updated.count()}")
    to_insert.write.format("delta").mode("append").save(silver_path)
 
print("SCD2 step complete.")
print(f"Total Silver rows now (current + history): {spark.read.format('delta').load(silver_path).count()}")
print(f"Currently active rows (is_current=true): {spark.read.format('delta').load(silver_path).filter('is_current=true').count()}")