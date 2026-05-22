# Databricks notebook source
# MAGIC %run ./00_setup
# MAGIC

# COMMAND ----------

# 00_orchestrator — runs every HR capstone notebook in order
dbutils.widgets.text("run_id",   "manual_orchestrator_run")
dbutils.widgets.text("run_date", "2026-05-21")
run_id   = dbutils.widgets.get("run_id")
run_date = dbutils.widgets.get("run_date")
 
NOTEBOOKS = [
    "10_bronze_attendance",
    "11_bronze_payroll",
    "12_bronze_employee",
    "13_bronze_department",
    "20_silver_attendance",
    "21_silver_payroll",
    "22_silver_employee_scd2",
    "23_silver_department",
    "30_gold_workforce_summary",
    "31_gold_dept_headcount_history",
    "32_gold_attendance_payroll",
    "40_push_to_azuresql",
]
 
print(f"Orchestrator run_id: {run_id}")
print(f"Processing run_date: {run_date}")
print(f"Will execute {len(NOTEBOOKS)} notebooks in order")

# COMMAND ----------

# Cell 2 — sequential execution + audit logging
import time
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

success_count = 0
fail_count = 0
results = []

for nb in NOTEBOOKS:
    print(f"\n{'='*60}\n  Running {nb}\n{'='*60}")
    start = time.time()
    try:
        result = dbutils.notebook.run(
            nb,
            timeout_seconds=900,
            arguments={"run_id": run_id, "run_date": run_date}
        )
        duration = time.time() - start
        success_count += 1
        results.append((nb, "SUCCESS", duration, result))
        print(f"  ✓ {nb} succeeded in {duration:.1f}s")
    except Exception as e:
        duration = time.time() - start
        fail_count += 1
        results.append((nb, "FAILED", duration, str(e)))
        print(f"  ✗ {nb} FAILED: {e}")
        # Continue with remaining notebooks rather than stopping entirely
        # In production you might want to fail fast — change to 'raise' here.

print(f"\n{'='*60}\n  ORCHESTRATOR SUMMARY\n{'='*60}")
print(f"  Succeeded: {success_count}")
print(f"  Failed:    {fail_count}")
for nb, status, dur, _ in results:
    print(f"    {status:8s}  {dur:6.1f}s  {nb}")

# ====== Write audit row to dbo.pipeline_audit (runs whether or not anything failed) ======

total_duration_sec = sum(dur for _, _, dur, _ in results)
end_time   = datetime.utcnow()
start_time = end_time - timedelta(seconds=total_duration_sec)

# Get record counts from Silver / rejected tables
def safe_count(path):
    try:
        return spark.read.format("delta").load(path).count()
    except Exception:
        return 0

records_attendance  = safe_count(f"{BASE}/silver/attendance")
records_payroll     = safe_count(f"{BASE}/silver/payroll")
records_employee    = safe_count(f"{BASE}/silver/employee")
records_total       = records_attendance + records_payroll + records_employee

rejected_attendance = safe_count(f"{BASE}/rejected/attendance")
rejected_payroll    = safe_count(f"{BASE}/rejected/payroll")
records_rejected    = rejected_attendance + rejected_payroll

failed_notebooks = [nb for nb, status, _, _ in results if status == "FAILED"]

# Explicit schema — avoids "Some types cannot be determined" error from None values
audit_schema = StructType([
    StructField("run_id",           StringType(),    False),
    StructField("pipeline_name",    StringType(),    True),
    StructField("source_name",      StringType(),    True),
    StructField("load_type",        StringType(),    True),
    StructField("start_time",       TimestampType(), True),
    StructField("end_time",         TimestampType(), True),
    StructField("records_read",     IntegerType(),   True),
    StructField("records_inserted", IntegerType(),   True),
    StructField("records_updated",  IntegerType(),   True),
    StructField("records_rejected", IntegerType(),   True),
    StructField("status",           StringType(),    True),
    StructField("error_message",    StringType(),    True),
])

audit_data = [(
    run_id,
    "pl_master_hr",
    "all_sources",
    "Full",
    start_time,
    end_time,
    records_total,
    records_total,
    0,
    records_rejected,
    "Success" if fail_count == 0 else "PartialFailure",
    None if fail_count == 0 else f"Failed notebooks: {', '.join(failed_notebooks)}",
)]

audit_row = spark.createDataFrame(audit_data, schema=audit_schema)

(audit_row.write
   .mode("append")
   .jdbc(jdbc_url, "dbo.pipeline_audit", properties=jdbc_props))

print(f"\nAudit row written for run_id: {run_id}")
print(f"  records_read: {records_total}, rejected: {records_rejected}")
print(f"  status: {'Success' if fail_count == 0 else 'PartialFailure'}")

if fail_count > 0:
    raise Exception(f"{fail_count} notebooks failed in orchestrator run")
else:
    dbutils.notebook.exit("SUCCESS")

# COMMAND ----------



# COMMAND ----------

