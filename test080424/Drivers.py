# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/sanly/landmark/raw/

# COMMAND ----------

df = spark.read.json('dbfs:/mnt/sanly/landmark/raw/drivers.json')

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_final = df.withColumn("forename",col("name.forename"))\
.withColumn("surname",col("name.surname"))\
.drop("name","url")\
    .withColumnRenamed("driverId","driver_id")\
    .withColumn('Ingestion_date',current_timestamp())


# COMMAND ----------

df_final.write.parquet("dbfs:/mnt/sanly/landmark/process_data/surya/driver")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table surya.drivers as
# MAGIC select * from parquet.`dbfs:/mnt/sanly/landmark/process_data/surya/driver`

# COMMAND ----------

df2 = spark.read.csv('dbfs:/mnt/sanly/landmark/raw/Baby_Names.csv')

# COMMAND ----------

df2.show()

# COMMAND ----------

users_schema="Year int, First_Name string, Country string, Sex string, Count int"
df=spark.read.schema(users_schema).csv("dbfs:/mnt/sanly/landmark/raw/Baby_Names.csv",header=True)

# COMMAND ----------

df.display()
