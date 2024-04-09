# Databricks notebook source
# MAGIC %run /Workspace/Users/navallyemul@gmail.com/Spark/includes

# COMMAND ----------


df=spark.read.csv(f"{input_path}/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table surya.circuit

# COMMAND ----------

df_final=df.withColumn("ingestion_date",current_timestamp())\
.withColumn("source_path",input_file_name()).drop("url")

# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable("surya.circuit")

# COMMAND ----------

(
spark
 .read
 .csv(f"{input_path}/Baby_Names.csv",header=True,inferSchema=True)
 .write
.option("delta.columnMapping.mode","name")
 .saveAsTable("surya.baby_name_broze")
 )

# COMMAND ----------

df=spark.read.table("surya.baby_name_broze")

# COMMAND ----------

df.where("Year=2007 and Count in (15,14)").display()

# COMMAND ----------

df.orderBy(desc('Year'),desc('First Name')).display()

# COMMAND ----------

df.orderBy(col('Year').asc()).display()

# COMMAND ----------

df.orderBy('Year', ascending = True).display()
