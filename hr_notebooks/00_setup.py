# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('kv-capstone-scope')

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC databricks secrets list-acls --scope kv-capstone-scope

# COMMAND ----------

storage_account = "stcapstonehr1123"   # <-- REPLACE with your actual storage account name
container       = "landing"
 
# Pull storage key from Key Vault via secret scope
storage_key = dbutils.secrets.get(scope="kv-capstone-scope", key="storage-account-key")
 
# Configure Spark to use the key when accessing ADLS via abfss://
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)
 
# BASE path — every other notebook reads/writes under this prefix
BASE = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
 
print(f"BASE path: {BASE}")
print("Top-level contents of landing container:")
display(dbutils.fs.ls(BASE))

# COMMAND ----------

# JDBC config for writing to Azure SQL — used by audit logging and the gold-to-SQL push
sql_server = "sql-capstone-hr-ups.database.windows.net"  # <-- REPLACE
sql_db     = "hrdb"
 
jdbc_url = (
    f"jdbc:sqlserver://{sql_server}:1433;"
    f"database={sql_db};"
    "encrypt=true;trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
)
 
jdbc_props = {
    "user":     dbutils.secrets.get("kv-capstone-scope", "sql-admin-user"),
    "password": dbutils.secrets.get("kv-capstone-scope", "sql-admin-password"),
    "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}
 
print(f"JDBC ready for {sql_server}/{sql_db}")

# COMMAND ----------

