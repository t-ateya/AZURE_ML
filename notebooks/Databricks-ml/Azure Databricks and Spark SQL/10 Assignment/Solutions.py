# Databricks notebook source
# MAGIC %md
# MAGIC # Assignment 2 - Employees (Solution)
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Mount the employees container to DBFS

# COMMAND ----------

#Insert your scope and key in the secrets.get method
#ensure you have set up the secret scopes and stored your IDs and keys in the key vault
application_id = dbutils.secrets.get(scope="databricks-secrets-ateya", key="application-id")
tenant_id = dbutils.secrets.get(scope="databricks-secrets-ateya", key="tenant-id")
secret = dbutils.secrets.get(scope="databricks-secrets-ateya", key="secret")


# COMMAND ----------

container_name = "employees"
storage_account_name = "datalakestorageateya"
mount_point = "/mnt/employees"

# COMMAND ----------

# Ensure your secret is still valid, if it has expired you will have to re-generate the secret and store it in the key vault
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{application_id}",
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding parquet files to the silver folder of the employees container

# COMMAND ----------

# MAGIC %md
# MAGIC ### Employees

# COMMAND ----------

# Importing the data the data types
from pyspark.sql.types import DoubleType, StringType, IntegerType, StructField, StructType, DateType 

# COMMAND ----------

# Reading in the employees csv file from the bronze layer 
# Reading in the hire_data
employees_path = '/mnt/employees/bronze/employees.csv'
spark.read.csv(path=employees_path, header=True).display()

# COMMAND ----------



employees_schema = StructType([
    StructField("EMPLOYEE_ID", IntegerType(), False),
    StructField("FIRST_NAME", StringType(), False),
    StructField("LAST_NAME", StringType(), False),
    StructField("EMAIL", StringType(), False),
    StructField("PHONE_NUMBER", StringType(), False),
    StructField("HIRE_DATE", StringType(), False),
    StructField("JOB_ID", StringType(), False),
    StructField("SALARY", StringType(), False),
    StructField("MANAGER_ID",IntegerType(), True),
    StructField("DEPARTMENT_ID", IntegerType(), False)
])

employees = spark.read.csv(path=employees_path, header=True, schema=employees_schema)

# COMMAND ----------

employees.display()

# COMMAND ----------

# Dropping unnecessary columns
employees = employees.drop("EMAIL", "PHONE_NUMBER")

# COMMAND ----------

employees.display()

# COMMAND ----------

# Changing the data type of hire_date from string to data via the to_date function
from pyspark.sql.functions import to_date 
employees = employees.select(
    "EMPLOYEE_ID",
    "FIRST_NAME",
    "LAST_NAME",
    to_date(employees["HIRE_DATE"], "MM/dd/yyyy").alias('HIRE_DATE'),
    "JOB_ID",
    "SALARY",
    "MANAGER_ID",
    "DEPARTMENT_ID"
)


# COMMAND ----------

employees.display()

# COMMAND ----------

# Writing dataframe as parquet file to silver layer
employees.write.parquet("/mnt/employees/silver/employees", mode='overwrite')


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Department

# COMMAND ----------

#Read the department csv from the bronze layer
dept_path = "/mnt/employees/bronze/departments.csv"

# COMMAND ----------

spark.read.csv(path=dept_path, header=True).display()

# COMMAND ----------

dept_schema = StructType(
    [
        StructField("DEPARTMENT_ID", IntegerType(), False),
        StructField("DEPARTMENT_NAME", StringType(), False),
        StructField("MANAGER_ID", IntegerType(), False),
        StructField("LOCATION_ID", IntegerType(), False)

    ]
)
dept = spark.read.csv(path=dept_path, schema=dept_schema, header=True)

# COMMAND ----------

dept.display()

# COMMAND ----------

# Dropping unnecessary columns
dept = dept.drop("MANAGER", "LOCATION_ID")

# COMMAND ----------

dept.display()

# COMMAND ----------

#Writing dataframe as parquet file to silver layer
dept.write.parquet("/mnt/employees/silver/departments", mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Countries

# COMMAND ----------

# Reading the cvs file from the bronze layer
countries_path = "/mnt/employees/bronze/countries.csv"
spark.read.csv(path=countries_path, header=True).display()



# COMMAND ----------

countries_schema = StructType(
    [
        StructField("COUNTRY_ID", StringType(), False),
        StructField("COUNTRY_NAME", StringType(), False)
    ]
)

countries = spark.read.csv(path=countries_path, header=True, schema=countries_schema)

# COMMAND ----------

countries.display()

# COMMAND ----------

# Write the parquet file to silver 
countries.write.parquet("/mnt/employees/silver/countries", mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Adding parquet files to the gold folder of the employees container

# COMMAND ----------

# MAGIC %md
# MAGIC ### Employees

# COMMAND ----------

employees = spark.read.parquet("/mnt/employees/silver/employees")

# COMMAND ----------

employees.display()

# COMMAND ----------

# Create a new column called full_name via the withColumn method and using the concat_ws function
from pyspark.sql.functions import concat_ws 
employees = employees.withColumn("FULL_NAME", concat_ws(' ', employees['FIRST_NAME'], employees['LAST_NAME']))

# COMMAND ----------

employees.display()

# COMMAND ----------

# Dropping unnecessary columns
employees = employees.drop("FIRST_NAME", "LAST_NAME", "MANAGER_ID")

# COMMAND ----------

employees.display()

# COMMAND ----------

# Reading in the departments parquet file from the silver layer
departments = spark.read.parquet("/mnt/employees/silver/departments")

# COMMAND ----------

departments.display()

# COMMAND ----------

# Joining the employees and departments silver tables to include the relevant fields such as department id, and drop columns that not required
employees = employees.join(departments, employees['department_id']==departments['department_id'], 'left') \
    .select('EMPLOYEE_ID', 'FULL_NAME', "HIRE_DATE", "JOB_ID", "SALARY", "DEPARTMENT_NAME")

# COMMAND ----------

# Writing the employees dataframe to the gold layer
employees.write.parquet("/mnt/employees/gold/employees", mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating the employees database and loading the gold layer table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE employees.employees
# MAGIC (
# MAGIC   EMPLOYEE_ID int,
# MAGIC   FULL_NAME string,
# MAGIC   HIRE_DATE date,
# MAGIC   JOB_ID string,
# MAGIC   SALARY int,
# MAGIC   DEPARTMENT_NAME string
# MAGIC
# MAGIC )
# MAGIC
# MAGIC USING parquet
# MAGIC LOCATION '/mnt/employees/gold/employees'

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP  TABLE IF EXISTS  employees.employees

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM employees.employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED employees.employees;

# COMMAND ----------

