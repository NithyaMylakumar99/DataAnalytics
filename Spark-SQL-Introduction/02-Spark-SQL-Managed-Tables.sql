-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Data File Path in DataLake Storage Account
-- MAGIC
-- MAGIC CSV Source File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/csv"
-- MAGIC
-- MAGIC JSON Source  File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/json"
-- MAGIC
-- MAGIC PARQUET Source  File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/parquet"
-- MAGIC
-- MAGIC
-- MAGIC ###### Spark Session Methods
-- MAGIC - <a href="https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.SparkSession.html" target="_blank">SparkSessionMethods</a>  **`read`**,**`write`**, **`createDataFrame`** , **`sql`** ,  **`table`**   
-- MAGIC
-- MAGIC ###### Dataframes To/From SQL Conversions
-- MAGIC - <a href="https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrame.createOrReplaceTempView.html" target="_blank">DataFrame-SQLConversions</a> :**`createOrReplaceTempView`** ,**`spark.sql`**  ,**`createOrReplaceGlobalTempView`**

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


SELECT * FROM global_temp.daily_pricing_global

-- COMMAND ----------

create table daily_pricing_csv_managed AS
SELECT * FROM global_temp.daily_pricing_global

-- COMMAND ----------

SELECT COUNT(*) FROM daily_pricing_csv_managed

-- COMMAND ----------

INSERT INTO daily_pricing_csv_managed 
SELECT * FROM global_temp.daily_pricing_global

-- COMMAND ----------

ALTER TABLE daily_pricing_csv_managed 
ADD COLUMN DATALAKE_UPDATED_DATE DATE

-- COMMAND ----------

SELECT * FROM daily_pricing_csv_managed

-- COMMAND ----------

UPDATE daily_pricing_csv_managed
SET DATALAKE_UPDATED_DATE = current_timestamp()

-- COMMAND ----------

DESCRIBE EXTENDED daily_pricing_csv_managed


-- COMMAND ----------

drop table daily_pricing_csv_managed

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sourceCSVFileDF = (spark.sql("SELECT * FROM global_temp.daily_pricing_global")
-- MAGIC                    )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (sourceCSVFileDF
-- MAGIC  .write.saveAsTable("daily_pricing_csv_managed")
-- MAGIC )
