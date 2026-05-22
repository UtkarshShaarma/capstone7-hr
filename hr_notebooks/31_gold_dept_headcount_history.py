# Databricks notebook source
# MAGIC %run ./00_setup

# COMMAND ----------

dbutils.widgets.text("run_id",   "manual_test_run")
dbutils.widgets.text("run_date", "2026-05-21")
run_id   = dbutils.widgets.get("run_id")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

from pyspark.sql import functions as F
 
emp = spark.read.format("delta").load(f"{BASE}/silver/employee")
 
# Generate month-end dates for the last 12 months
months = (spark.range(0, 12)
    .select(F.date_format(F.last_day(F.add_months(F.current_date(), -F.col("id").cast("int"))), "yyyy-MM").alias("snapshot_month")))
 
# For each month-end, count active employees per department
# An employee is "active at month X" if:
#   - their effective_from <= last_day(month X)
#   - their effective_to is NULL OR effective_to > last_day(month X)
#   - their employment_status was Active at that version
snapshots = (months.alias("m")
    .crossJoin(emp.alias("e"))
    .filter("""
        e.effective_from <= last_day(to_date(concat(m.snapshot_month, '-01')))
        AND (e.effective_to IS NULL
             OR e.effective_to > last_day(to_date(concat(m.snapshot_month, '-01'))))
        AND e.employment_status = 'Active'
    """)
    .groupBy("e.department_id", "m.snapshot_month")
    .agg(F.count("*").alias("headcount"))
    .withColumn("refresh_ts", F.current_timestamp()))
 
(snapshots.write.format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .save(f"{BASE}/gold/dept_headcount_history"))
 
print(f"Gold dept_headcount_history: {snapshots.count()} rows")
display(snapshots.orderBy("snapshot_month", "department_id"))

# COMMAND ----------

