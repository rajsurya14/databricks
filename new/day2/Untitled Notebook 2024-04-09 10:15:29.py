# Databricks notebook source
emp = [(1, "AAA", "dept1", 1000, "2019-02-01 15:12:13"),
    (2, "BBB", "dept1", 1100, "2018-04-01 5:12:3"),
    (3, "CCC", "dept1", 3000, "2017-06-05 1:2:13"),
    (4, "DDD", "dept1", 1500, "2019-08-10 10:52:53"),
    (5, "EEE", "dept2", 8000, "2016-01-11 5:52:43"),
    (6, "FFF", "dept2", 7200, "2015-04-14 19:32:33"),
    (7, "GGG", "dept3", 7100, "2019-02-21 15:42:43"),
    (8, "HHH", "dept3", 3700, "2016-09-25 15:32:33"),
    (9, "III", "dept3", 4500, "2017-10-15 15:22:23"),
    (10, "JJJ", "dept5", 3400, "2018-12-17 15:14:17")]
empdf = spark.createDataFrame(emp, ["id", "name", "dept", "salary", "date"])
display(empdf)

# COMMAND ----------

from pyspark.sql.functions import *
empdf.withColumn("new_date",date_format("date","dd/MM/yy")).display()

# COMMAND ----------

schema_str = "id INT, name STRING, dept STRING, salary INT, date DATE"
empty_df = spark.createDataFrame([], schema_str)

# COMMAND ----------

empty_df.display()

# COMMAND ----------

empty_df.columns

# COMMAND ----------

empdf.withColumn('newdate',to_date(col('date'))).display()

# COMMAND ----------

empdf.printSchema()

# COMMAND ----------


