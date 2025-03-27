# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ##### Read CSV File from Azure Data Lake Storage Account
# MAGIC  CSV Source File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/csv"
# MAGIC
# MAGIC JSON  File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/json"
# MAGIC
# MAGIC
# MAGIC ##### Databricks Utilities
# MAGIC - <a href="https://docs.databricks.com/en/dev-tools/databricks-utils.html">dbutils</a>

# COMMAND ----------

storageAccountKey='6MiaWiwRhjy9f6QfTWkxOCRGd7sOg2seHbeck8uKpGUOR6ZpVoUPppYW+6SIdp6oNrUa9OrgGsgY+AStvN0KnA=='
spark.conf.set("fs.azure.account.key.adlsadataengdev.dfs.core.windows.net",storageAccountKey)

# COMMAND ----------

sourceCSVFilePath = 'abfss://working-labs@adlsadataengdev.dfs.core.windows.net/bronze/daily-pricing/csv'
sourceJSONFilePath = 'abfss://working-labs@adlsadataengdev.dfs.core.windows.net/bronze/daily-pricing/json'
sourcePARQUETFilePath = 'abfss://working-labs@adlsadataengdev.dfs.core.windows.net/bronze/daily-pricing/parquet'

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help("ls")

# COMMAND ----------

dbutils.fs.ls(sourceCSVFilePath
              )
