# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/raw/

# COMMAND ----------

# MAGIC %sql
# MAGIC create database surya

# COMMAND ----------

df = spark.read.csv('dbfs:/FileStore/tables/raw/circuits.csv',header=True,inferSchema=True)
df.write.saveAsTable('surya.circuit')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from surya.circuit

# COMMAND ----------

from pyspark.sql.functions import col
df.select(col('circuitId').alias('circuit_id')).display()

# COMMAND ----------

from pyspark.sql.functions import *
df.select("*",concat("location",lit(""),"country").alias('loc_country')).display()

# COMMAND ----------

df.withColumnRenamed('circuitId','circuit_Id').display()

# COMMAND ----------

new_columns= ['circuit_id',
 'circuit_ref',
 'name',
 'location',
 'country',
 'latitude',
 'longitude',
 'altitude',
 'url']

# COMMAND ----------

df1 = df.toDF(*new_columns)

# COMMAND ----------

df1.display()

# COMMAND ----------

df.withColumn('ingestiondate',current_date()).display()

# COMMAND ----------

df.withColumn('location',upper('location')).display()
