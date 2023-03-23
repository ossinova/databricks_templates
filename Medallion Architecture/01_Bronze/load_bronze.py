# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Loading Bronze (Raw) data

# COMMAND ----------

# Parameters
dbutils.widgets.text("TableName", "")
dbutils.widgets.text("TablePath", "")
dbutils.widgets.text("SourceSystem", "")
dbutils.widgets.text("DataFormat", "")
dbutils.widgets.dropdown("LoadType", "overwrite", ["overwrite", "append"])

# Get Parameters
TABLENAME = dbutils.widgets.get("TableName")
TABLEPATH = dbutils.widgets.get("TablePath")
SOURCE = dbutils.widgets.get("SourceSystem")
FORMAT = dbutils.widgets.get("DataFormat")
LOAD = dbutils.widgets.get("LoadType")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # From ADLSv2

# COMMAND ----------

# Setup credentials (could also be configured in cluster settings)

service_credential = dbutils.secrets.get(scope="<scope>",key="<service-credential-key>")

spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", "<application-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")


# Assuming you want EXTERNAL TABLES

conatinerName = ""
storageaccountName = ""
landingPath = f"abfss://{conatinerName}@{storageaccountName}.dfs.core.windows.net/landing"
bronzePath = f"abfss://{conatinerName}@{storageaccountName}.dfs.core.windows.net/bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load
# MAGIC 
# MAGIC Loads data from: abfss://{container}@{storageaccount}.dfs.core.windows.net/landing/{TABLEPATH} -> TABLEPATH could be: /2023/03/31/table1 or /source1/table1

# COMMAND ----------

df = spark.read.parquet(f"{landingPath}/{TABLEPATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Save
# MAGIC 
# MAGIC Saves to delta lake: abfss://{container}@{storageaccount}.dfs.core.windows.net/bronze/{SOURCE}/{TABLENAME}

# COMMAND ----------

(
df.write.format("delta")
    .option("overwriteSchema", "true")
    .option("path": f"{bronzePath}/{SOURCE}/{TABLENAME}") # EXTERNAL TABLE
    .mode(LOAD) # append or overwrite
    .saveAsTable(f"{catalogName}.{SOURCE}_{TABLENAME}")
)
