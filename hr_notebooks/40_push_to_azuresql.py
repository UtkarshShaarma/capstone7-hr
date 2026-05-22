# Databricks notebook source
# MAGIC %run ./00_setup

# COMMAND ----------

dbutils.widgets.text("run_id",   "manual_test_run")
dbutils.widgets.text("run_date", "2026-05-21")
run_id   = dbutils.widgets.get("run_id")
run_date = dbutils.widgets.get("run_date")


# COMMAND ----------

# Cell 2 — JDBC write Gold → rpt_ tables in Azure SQL
gold_to_sql = [
    ("rpt_workforce_summary",      f"{BASE}/gold/workforce_summary"),
    ("rpt_dept_headcount_history", f"{BASE}/gold/dept_headcount_history"),
    ("rpt_attendance_payroll",     f"{BASE}/gold/attendance_payroll"),
]
 
for tbl, path in gold_to_sql:
    print(f"Writing {tbl} ...")
    df = spark.read.format("delta").load(path)
    cnt = df.count()
    (df.write
       .mode("overwrite")
       .option("truncate", "true")   # truncate rather than drop+recreate (preserves indexes)
       .jdbc(jdbc_url, f"dbo.{tbl}", properties=jdbc_props))
    print(f"  Wrote {cnt} rows to dbo.{tbl}")
 
print("All Gold tables pushed to Azure SQL.")

# COMMAND ----------

