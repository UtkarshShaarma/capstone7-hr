# Databricks notebook source
# MAGIC %run ./00_setup

# COMMAND ----------

dbutils.widgets.text("run_id",   "manual_test_run")
dbutils.widgets.text("run_date", "2026-05-21")
run_id   = dbutils.widgets.get("run_id")
run_date = dbutils.widgets.get("run_date")


# COMMAND ----------

from pyspark.sql import functions as F
 
# Read Silver — only current rows from SCD2
emp  = spark.read.format("delta").load(f"{BASE}/silver/employee").filter("is_current = true")
dept = spark.read.format("delta").load(f"{BASE}/silver/department")
 
gold = (emp.join(dept, "department_id", "left")
   .groupBy("department_id", "department_name")
   .agg(
       F.count("*").alias("headcount"),
       F.sum(F.when(F.col("employment_status") == "Active", 1).otherwise(0)).alias("active_count"),
       F.round(F.avg("salary"), 2).alias("avg_salary"),
       F.sum(F.when(F.col("employment_status").isin("Resigned", "Notice"), 1).otherwise(0)).alias("attrition_count"))
   .withColumn("refresh_ts", F.current_timestamp()))
 
(gold.write.format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .save(f"{BASE}/gold/workforce_summary"))
 
print(f"Gold workforce_summary: {gold.count()} departments")
display(gold.orderBy(F.desc("attrition_count")))


# COMMAND ----------

