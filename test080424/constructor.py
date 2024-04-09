# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/raw/

# COMMAND ----------

df1 = spark.read.json('dbfs:/FileStore/tables/raw/constructors.json')

# COMMAND ----------

df1.write.saveAsTable('surya.constructors')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from surya.constructors

# COMMAND ----------


