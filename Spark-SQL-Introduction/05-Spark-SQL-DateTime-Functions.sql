-- Databricks notebook source
-- DBTITLE 0,--i18n-ef4d95c5-f516-40e2-975d-71fc17485bba
-- MAGIC %md
-- MAGIC
-- MAGIC ##### Read CSV File from Azure Data Lake Storage Account
-- MAGIC  CSV Source File Path : "abfss://DatalakeStorageAccountName.dfs.core.windows.net/daily-pricing"
-- MAGIC
-- MAGIC JSON Source  File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/json"
-- MAGIC
-- MAGIC PARQUET Source  File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/parquet"
-- MAGIC
-- MAGIC ###### Spark Session Methods
-- MAGIC - <a href="https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.SparkSession.html" target="_blank">SparkSessionMethods</a> :**`read`**,**`write`**,  **`sql`** ,  **`table`** ,  **`createDataFrame`**
-- MAGIC
-- MAGIC
-- MAGIC ##### DateTime Methods
-- MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#datetime-functions" target="_blank">Built-In DateTime Functions</a>: **`date_format`**, **`to_date`**, **`date_add`**, **`year`**, **`month`**, **`dayofweek`**, **`minute`**, **`second`**

-- COMMAND ----------

-- MAGIC %python
-- MAGIC storageAccountKey='6MiaWiwRhjy9f6QfTWkxOCRGd7sOg2seHbeck8uKpGUOR6ZpVoUPppYW+6SIdp6oNrUa9OrgGsgY+AStvN0KnA=='
-- MAGIC spark.conf.set("fs.azure.account.key.adlsadataengdev.dfs.core.windows.net",storageAccountKey)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sourceCSVFilePath = 'abfss://working-labs@adlsadataengdev.dfs.core.windows.net/bronze/daily-pricing/csv'
-- MAGIC sourceJSONFilePath = 'abfss://working-labs@adlsadataengdev.dfs.core.windows.net/bronze/daily-pricing/json'
-- MAGIC sourcePARQUETFilePath = 'abfss://working-labs@adlsadataengdev.dfs.core.windows.net/bronze/daily-pricing/parquet'

-- COMMAND ----------

SELECT * FROM daily_pricing_json_external_table

-- COMMAND ----------

SELECT *, current_timestamp() as table_updated_date FROM 
daily_pricing_json_external_table

-- COMMAND ----------

SELECT *, current_timestamp() as table_updated_date,
year(current_timestamp()) as table_update_year FROM 
daily_pricing_json_external_table

-- COMMAND ----------

SELECT *, current_timestamp() as table_updated_date,
year(current_timestamp()) as table_update_year,
month(current_timestamp()) as table_update_month FROM 
daily_pricing_json_external_table

-- COMMAND ----------

SELECT *, 
current_timestamp() as table_updated_date,
date_format(current_timestamp(), 'yyyyMMDD')
as table_update_date_format FROM 
daily_pricing_json_external_table

-- COMMAND ----------

SELECT date_of_pricing
, to_date(date_of_pricing, 'dd/MM/yyyy') as PRICING_DATE
FROM daily_pricing_json_external_table 


-- COMMAND ----------


