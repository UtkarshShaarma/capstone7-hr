# Databricks notebook source
# MAGIC %run ./00_setup

# COMMAND ----------

dbutils.widgets.text("run_id",   "manual_test_run")
dbutils.widgets.text("run_date", "2026-05-21")
run_id   = dbutils.widgets.get("run_id")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

# DBTITLE 1,Re-ingest Bronze data
# Re-ingest Bronze attendance data (source was deleted by previous run)
from pyspark.sql import functions as F

src_path = f"{BASE}/raw/attendance/2026-05-20/attendance.csv"
raw = (spark.read.option("header", True).option("inferSchema", True).csv(src_path))

bronze_reingest = (raw
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_source_file", F.expr("_metadata.file_path"))
    .withColumn("_run_id", F.lit(run_id)))

bronze_path = f"{BASE}/bronze/attendance"
(bronze_reingest.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(bronze_path))

print(f"Re-ingested {bronze_reingest.count()} rows to {bronze_path}")

# COMMAND ----------

# Cell 2 — read Bronze, dedupe by attendance_id (handles re-ingests only)
from pyspark.sql import functions as F, Window

bronze = spark.read.format("delta").load(f"{BASE}/bronze/attendance")
print(f"Bronze rows (all history): {bronze.count()}")

# Dedupe by attendance_id only — keep latest ingest of each unique source row
# This handles "we ingested the same file twice" but NOT real source DQ issues.
latest_window = Window.partitionBy("attendance_id").orderBy(F.desc("_ingest_ts"))
latest = (bronze
    .withColumn("_rn_ingest", F.row_number().over(latest_window))
    .filter("_rn_ingest = 1")
    .drop("_rn_ingest"))
print(f"After de-duplicating re-ingests: {latest.count()}")

# Type-cast and compute work_hours
# check_in_time and check_out_time are already full timestamps in Bronze,
# so use them directly (no concat with attendance_date needed).
df = (latest
    .withColumn("check_in_ts", F.col("check_in_time"))
    .withColumn("check_out_ts", F.col("check_out_time"))
    .withColumn("attendance_date", F.to_date("attendance_date"))
    .withColumn("work_hours",
        (F.unix_timestamp("check_out_ts") - F.unix_timestamp("check_in_ts")) / 3600.0))

display(df.limit(5))

# COMMAND ----------

# Cell 3 — DQ flags + deduplication
# DQ Rule 1: "Absent" or "Leave" with check-in times is suspicious
df = df.withColumn("dq_status_with_times",
    F.when(F.col("attendance_status").isin("Absent", "Leave") & F.col("check_in_time").isNotNull(),
           F.lit("warn:absent_has_times"))
     .otherwise(F.lit(None)))
 
# DQ Rule 2: excessive work hours
df = df.withColumn("dq_excess_hours",
    F.when(F.col("work_hours") > 14, F.lit("warn:excess_hours"))
     .otherwise(F.lit(None)))
 
# Deduplicate (employee_id, attendance_date), keeping latest attendance_id
dedup_window = Window.partitionBy("employee_id", "attendance_date").orderBy(F.desc("attendance_id"))
df = df.withColumn("_rn", F.row_number().over(dedup_window))
 
duplicates = df.filter("_rn > 1").withColumn("rejection_reason", F.lit("duplicate_attendance"))
clean      = df.filter("_rn = 1").drop("_rn")
 
print(f"Clean rows:    {clean.count()}")
print(f"Duplicates:    {duplicates.count()}")
print(f"DQ warn rows (kept but flagged): {clean.filter('dq_status_with_times IS NOT NULL OR dq_excess_hours IS NOT NULL').count()}")

# COMMAND ----------

# Cell 4 — persist Silver and Rejected
# Write rejected duplicates to a parallel rejected zone
(duplicates
   .select("attendance_id", "employee_id", "attendance_date",
           "attendance_status", "rejection_reason", "_ingest_ts", "_run_id")
   .write.format("delta")
   .mode("append")
   .save(f"{BASE}/rejected/attendance"))
 
# Write clean rows to Silver (overwrite — Silver is rebuildable from Bronze)
(clean.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(f"{BASE}/silver/attendance"))
 
print("Silver and Rejected written successfully.")
print(f"Verifying Silver: {spark.read.format('delta').load(f'{BASE}/silver/attendance').count()} rows")
print(f"Verifying Rejected: {spark.read.format('delta').load(f'{BASE}/rejected/attendance').count()} rows")

# COMMAND ----------

