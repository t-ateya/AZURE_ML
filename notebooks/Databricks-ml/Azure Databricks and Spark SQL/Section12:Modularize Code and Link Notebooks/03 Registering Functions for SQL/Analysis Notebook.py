# Databricks notebook source
# MAGIC %run "./Utility Notebook"

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/bronze/order_items.csv', header=True, inferSchema=True)
df.display()

# COMMAND ----------

df.createOrReplaceTempView('temp')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, multiply_cols(unit_price, quantity) as line_amount from temp;

# COMMAND ----------

