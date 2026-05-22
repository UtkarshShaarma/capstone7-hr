# Databricks notebook source
# MAGIC %run ./00_setup
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show every version of the Silver employee table
# MAGIC DESCRIBE HISTORY delta.`abfss://landing@stcapstonehr1123.dfs.core.windows.net/silver/employee`

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare current headcount to historical version
# MAGIC SELECT 'now' AS snapshot, COUNT(*) AS active_count
# MAGIC FROM   delta.`abfss://landing@stcapstonehr1123.dfs.core.windows.net/silver/employee`
# MAGIC WHERE  is_current = true AND employment_status = 'Active'
# MAGIC UNION ALL
# MAGIC SELECT 'v0', COUNT(*)
# MAGIC FROM   delta.`abfss://landing@stcapstonehr1123.dfs.core.windows.net/silver/employee`
# MAGIC VERSION AS OF 0
# MAGIC WHERE  is_current = true AND employment_status = 'Active';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT employment_status, COUNT(*) AS cnt
# MAGIC FROM   delta.`abfss://landing@stcapstonehr1123.dfs.core.windows.net/silver/employee`
# MAGIC WHERE  is_current = true
# MAGIC GROUP  BY employment_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compact small files + sort data on common query columns
# MAGIC OPTIMIZE delta.`abfss://landing@stcapstonehr1123.dfs.core.windows.net/silver/employee`
# MAGIC   ZORDER BY (department_id, employee_id);
# MAGIC  
# MAGIC OPTIMIZE delta.`abfss://landing@stcapstonehr1123.dfs.core.windows.net/silver/payroll`
# MAGIC   ZORDER BY (employee_id, pay_period);
# MAGIC  
# MAGIC OPTIMIZE delta.`abfss://landing@stcapstonehr1123.dfs.core.windows.net/silver/attendance`
# MAGIC   ZORDER BY (employee_id, attendance_date);

# COMMAND ----------

