# Databricks notebook source
countries = spark.read.csv('/FileStore/tables/countries.csv', header=True)

# COMMAND ----------

countries.display()

# COMMAND ----------

countries.createTempView('countries_tv')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM countries_tv 

# COMMAND ----------

spark.sql(
    "SELECT * FROM countries_tv"
).display()

# COMMAND ----------

table_name = "countries_tv"


# COMMAND ----------

spark.sql(f"SELECT * FROM {table_name}") \
    .display()

# COMMAND ----------

countries.createOrReplaceTempView('countries_tv')

# COMMAND ----------

# MAGIC %md 
# MAGIC # Global Temp View

# COMMAND ----------

countries.createOrReplaceGlobalTempView('countries_gv')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.countries_gv

# COMMAND ----------

spark.sql("SELECT * FROM global_temp.countries_gv") \
    .display()

# COMMAND ----------

