# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Config file
# MAGIC 
# MAGIC Used for configuring reusable parameters, secrets and paths to various resources
# MAGIC 
# MAGIC Here you should use:
# MAGIC - dbutils.widgets.get()
# MAGIC - dbutils.scope.get()

# COMMAND ----------

resourceGroup = spark.conf.get("spark.databricks.clusterUsageTags.managedResourceGroup")
clusterName = spark.conf.get("spark.databricks.clusterUsageTags.clusterName")
clusterRegion = spark.conf.get("spark.databricks.clusterUsageTags.region")
sparkVersion = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
databricksID = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
