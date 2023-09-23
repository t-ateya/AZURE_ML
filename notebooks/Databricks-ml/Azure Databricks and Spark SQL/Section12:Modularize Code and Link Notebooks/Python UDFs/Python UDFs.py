# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# length is native spark function
def count_chars(col):
    return length(col)


# COMMAND ----------

def count_chars_python(col):
    return len(col)

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/bronze/customers.csv', header=True, inferSchema = True)

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('FULL_NAME_LENGTH', count_chars(df.FULL_NAME)).display()

# COMMAND ----------

df.withColumn('FULL_NAME_LENGTH', count_chars_python(df.FULL_NAME)).display()

# COMMAND ----------

# from pyspark.sql.functions import udf
count_chars_python = udf(count_chars_python)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limitations of UDFs

# COMMAND ----------

# This function is defined using native spark
def count_chars(col):
    return length(col)


# COMMAND ----------

#This function is defined using pure Python
def count_chars_python(col):
    return len(col)

# Declaring the UDF
count_chars_python = udf(count_chars_python)

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/bronze/customers.csv', header=True, inferSchema = True)
df.display()

# COMMAND ----------

df.count()

# COMMAND ----------

new_df = df 
for _ in range(100):
    df = df.union(new_df)

# COMMAND ----------

df.count()

# COMMAND ----------

