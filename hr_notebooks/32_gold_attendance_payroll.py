# Databricks notebook source
# MAGIC %run ./00_setup

# COMMAND ----------

dbutils.widgets.text("run_id",   "manual_test_run")
dbutils.widgets.text("run_date", "2026-05-21")
run_id   = dbutils.widgets.get("run_id")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

from pyspark.sql import functions as F
 
att = spark.read.format("delta").load(f"{BASE}/silver/attendance")
pay = spark.read.format("delta").load(f"{BASE}/silver/payroll")
 
# Aggregate attendance by (employee, month)
att_agg = (att
    .withColumn("pay_period", F.date_format("attendance_date", "yyyy-MM"))
    .groupBy("employee_id", "pay_period")
    .agg(
        F.sum(F.when(F.col("attendance_status") == "Present", 1).otherwise(0)).alias("days_present"),
        F.sum(F.when(F.col("attendance_status") == "Absent",  1).otherwise(0)).alias("days_absent")))
 
# Join with payroll on (employee, period)
gold = (pay
    .join(att_agg, ["employee_id", "pay_period"], "left")
    .select("employee_id", "pay_period",
            F.coalesce("days_present", F.lit(0)).alias("days_present"),
            F.coalesce("days_absent",  F.lit(0)).alias("days_absent"),
            "net_pay_corrected", "payroll_delay_days",
            F.current_timestamp().alias("refresh_ts")))
 
(gold.write.format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .save(f"{BASE}/gold/attendance_payroll"))
 
print(f"Gold attendance_payroll: {gold.count()} rows")
display(gold.limit(20))

# COMMAND ----------

