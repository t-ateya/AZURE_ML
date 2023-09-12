# Databricks notebook source
# MAGIC %md
# MAGIC #Assignment 2- Silver to Gold (Solutions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ORDER_DETAILS
# MAGIC Create an order_details table that contains the following attributes:
# MAGIC - ORDER ID
# MAGIC - ORDER DATE
# MAGIC - CUSTOMER ID
# MAGIC - STORE NAME
# MAGIC - TOTAL ORDER AMOUNT
# MAGIC
# MAGIC The table should be aggregated by ORDER ID, ORDER DATE, CUSTOMER ID and STORE NAME to show the TOTAL ORDER AMOUNT
# MAGIC Hint: Please consider the order of operation when finding the TOTAL ORDER AMOUNT

# COMMAND ----------

# import the functions and read silder data tables as DataFrame
from pyspark.sql.functions import *

orders = spark.read.parquet('/FileStore/tables/silver/orders')
order_items = spark.read.parquet('/FileStore/tables/silver/order_items')
products = spark.read.parquet('/FileStore/tables/silver/products')
customers = spark.read.parquet('/FileStore/tables/silver/customers')

# COMMAND ----------

# Display the orders dataframe to review the columns
orders.display()

# COMMAND ----------

# Datatype of the order_timestamp column is timestamp and needs to be changed to date
orders.dtypes

# COMMAND ----------

# Changing the order_timestamp from 'timestamp' to 'date' using the to_date function
order_details = orders.select(
    'ORDER_ID',
    to_date('order_time').alias("DATE"),
    'CUSTOMER_ID',
    'STORE_NAME'
)

# COMMAND ----------

# Reviewing the current state of the order details dataframe
order_details.display()

# COMMAND ----------

# Reviewing the columns of the order_items dataframe
order_items.display()

# COMMAND ----------

# Joining the order_details and order_items dataframe on the 'order_id' column of both tables
# selecting the relevant columns from the resulting dataframe and storing it bact to the order_details variable
order_details = order_details.join(order_items, order_items['order_id'] == order_details['order_id'], 'left') \
.select(order_details['ORDER_ID'], order_details['DATE'], order_details['CUSTOMER_ID'], order_details['STORE_NAME'], order_items['UNIT_PRICE'], order_items['QUANTITY'])

# COMMAND ----------

# Reviewing order details
order_details.display()

# COMMAND ----------

# Creating a a total amount column at the record level
order_details = order_details.withColumn('TOTAL_SALES_AMOUNT', order_details['UNIT_PRICE']*order_details['QUANTITY'])

# COMMAND ----------

#Reviewing the current state of the order_details dataframe
order_details.display()

# COMMAND ----------

# Grouping the order_details dataframe and taking the sum of the total amount, renaming this to 'TOTAL_ORDER_AMOUNT'
order_details = order_details \
    .groupBy('ORDER_ID', 'DATE', 'CUSTOMER_ID', 'STORE_NAME') \
    .sum('TOTAL_SALES_AMOUNT') \
    .withColumnRenamed('sum(TOTAL_SALES_AMOUNT)', 'TOTAL_ORDER_AMOUNT')

# COMMAND ----------

# Reviewing the current state of the order_details dataframe
order_details.display()

# COMMAND ----------

# Rounding the TOTAL_ORDER_AMOUNT to 2 dp
order_details = order_details.withColumn('TOTAL_ORDER_AMOUNT', round('TOTAL_ORDER_AMOUNT', 2))

# COMMAND ----------

# Writing the order_details dataframe as a parquet file in the gold layer
order_details.write.parquet('/FileStore/tables/gold/order_details', mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC ## MONTHLY_SALES
# MAGIC Create an aggregated table to show the month sales total and save it in the gold layer as a parquet file called MONTHLY_SALES.
# MAGIC
# MAGIC The table should have two columns:
# MAGIC - MONTH_YEAR- this should be in the format cyyyy-MM e.g 2020-10
# MAGIC - TOAL_SALES 
# MAGIC Display the sales total rounded to 2 dp and sorted in descending date order

# COMMAND ----------

# can use the date columns from the order_details table
order_details.display()

# COMMAND ----------

# Creating a column that extracts the month and year from the date column assigning the dataframe results back to the sales_with_month variable

sales_with_month = order_details.withColumn('MONTH_YEAR', date_format('DATE', 'yyyy-MM'))

# COMMAND ----------

sales_with_month.display()

# COMMAND ----------

monthly_sales = sales_with_month \
    .groupBy('MONTH_YEAR').sum('TOTAL_ORDER_AMOUNT') \
    .withColumn('TOTAL_SALES', round('sum(TOTAL_ORDER_AMOUNT)', 2)) \
    .sort(sales_with_month['MONTH_YEAR'].desc()) \
    .select('MONTH_YEAR', 'TOTAL_SALES')

# COMMAND ----------

monthly_sales.display()

# COMMAND ----------

# Writing the monthly_sales dataframe as a parquet file in the gold layer
monthly_sales.write.parquet('/FileStore/tables/gold/monthly_sales', mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC ## STORE_MONTHLY_SALES
# MAGIC Create an aggregated table to show the monthly sales today by store and save it in the gold layer as a parquet file called STORE_MONTHLY_SALES.
# MAGIC
# MAGIC The table should have two columns:
# MAGIC - MONTH_YEAR -this should be in the format yyy-MM e.g 2020-10
# MAGIC - STORE NAME
# MAGIC - TOTAL_SALES
# MAGIC
# MAGIC Display the sales total rounded to 2 dp and sorted in descending date order.

# COMMAND ----------

# We can leverage the intermediate dataframe called sales_with_month to extract the information we need 
sales_with_month.display()

# COMMAND ----------

# in addition to month_year you must also goup by store_name
store_monthly_sales = sales_with_month.groupBy('MONTH_YEAR', 'STORE_NAME').sum('TOTAL_ORDER_AMOUNT') \
    .withColumn('TOTAL_SALES', round('sum(TOTAL_ORDER_AMOUNT)', 2)) \
    .sort(sales_with_month['MONTH_YEAR'].desc()) \
    .select('MONTH_YEAR', 'STORE_NAME', 'TOTAL_SALES')

# COMMAND ----------

store_monthly_sales.display()

# COMMAND ----------

# Writing the store_monthly_sales dataframe as a parquet file in the gold layer
store_monthly_sales.write.parquet('/FileStore/tables/gold/store_monthly_sales', mode='overwrite')

# COMMAND ----------

