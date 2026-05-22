# Databricks notebook source
# MAGIC %run ./00_setup

# COMMAND ----------

dbutils.widgets.text("run_id",   "manual_test_run")
dbutils.widgets.text("run_date", "2026-05-21")
run_id   = dbutils.widgets.get("run_id")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

# Cell 2 — read Bronze, dedupe by payroll_id (handles re-ingests only)
from pyspark.sql import functions as F, Window

bronze = spark.read.format("delta").load(f"{BASE}/bronze/payroll")
print(f"Bronze rows (all history): {bronze.count()}")

# Dedupe by payroll_id only — keeps the latest ingest of each unique source row.
# This handles "the same file got ingested twice" but NOT real source DQ issues.
latest_window = Window.partitionBy("payroll_id").orderBy(F.desc("_ingest_ts"))
latest = (bronze
    .withColumn("_rn_ingest", F.row_number().over(latest_window))
    .filter("_rn_ingest = 1")
    .drop("_rn_ingest"))
print(f"After de-duplicating re-ingests: {latest.count()}")

# COMMAND ----------

# Cell 3 — net_pay correction + DQ enrichment
df = (latest
    # Cast numerics
    .withColumn("basic_salary",     F.col("basic_salary").cast("decimal(18,2)"))
    .withColumn("bonus_amount",     F.col("bonus_amount").cast("decimal(18,2)"))
    .withColumn("deduction_amount", F.col("deduction_amount").cast("decimal(18,2)"))
    # Preserve the original (broken) net_pay for audit
    .withColumn("net_pay_raw", F.col("net_pay").cast("decimal(18,2)"))
    # Recompute net_pay from the formula
    .withColumn("net_pay_corrected",
        (F.col("basic_salary") + F.col("bonus_amount") - F.col("deduction_amount"))
        .cast("decimal(18,2)"))
    # Variance: how wrong was the source value
    .withColumn("net_pay_variance", F.col("net_pay_raw") - F.col("net_pay_corrected"))
    # Date enrichment
    .withColumn("processed_date", F.to_date("processed_date"))
    # pay_period is already a date (first of month), so use last_day directly
    .withColumn("pay_period_end", F.last_day(F.col("pay_period")))
    .withColumn("payroll_delay_days", F.datediff("processed_date", "pay_period_end"))
    # DQ Rule: processed before period ended (logically impossible)
    .withColumn("dq_future_processing",
        F.when(F.col("payroll_delay_days") < 0, F.lit("warn:processed_before_period_end"))
         .otherwise(F.lit(None))))
 
# Show the variance stats — this is your demo moment
print("Net pay variance stats (how wrong was the source?):")
df.select("net_pay_variance").describe().show()
print(f"Rows with non-zero variance: {df.filter('abs(net_pay_variance) > 0.01').count()} / {df.count()}")
print(f"Rows flagged for future processing: {df.filter('dq_future_processing IS NOT NULL').count()}")

# COMMAND ----------

# Cell 4 — dedup (employee, pay_period) and write Silver + Rejected
dedup_window = Window.partitionBy("employee_id", "pay_period").orderBy(F.desc("processed_date"), F.desc("payroll_id"))
df = df.withColumn("_rn", F.row_number().over(dedup_window))
 
duplicates = df.filter("_rn > 1").withColumn("rejection_reason", F.lit("duplicate_pay_period"))
clean      = df.filter("_rn = 1").drop("_rn")
 
print(f"Clean rows: {clean.count()}")
print(f"Duplicates rejected: {duplicates.count()}")
 
# Write rejected
(duplicates
   .select("payroll_id", "employee_id", "pay_period", "rejection_reason",
           "net_pay_raw", "net_pay_corrected", "_ingest_ts", "_run_id")
   .write.format("delta")
   .mode("append")
   .save(f"{BASE}/rejected/payroll"))
 
# Write Silver
(clean.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(f"{BASE}/silver/payroll"))
 
print(f"Silver payroll rows: {spark.read.format('delta').load(f'{BASE}/silver/payroll').count()}")

# COMMAND ----------

