# Databricks notebook source
# MAGIC %md
# MAGIC # Selecting and Renaming Columns

# COMMAND ----------


countries_path= "/FileStore/tables/countries.csv"

countries_df = spark.read.csv(countries_path, header="True")
display(countries_df)

# COMMAND ----------

from pyspark.sql.types import IntegerType, DoubleType, StructField, StructType, StringType
# FALSE => It should not be NUll, True=> It can be NULL
# StructType defines the structure of the DataFrame
# StructField defines the metadata of the columns within a DataFrame

counties_schema = StructType(
[
 StructField('COUNTRY_ID', IntegerType(), False),
 StructField('NAME', StringType(), False),
 StructField('NATIONALITY', StringType(), False),
 StructField('COUNTRY_CODE', StringType(), False),
 StructField('ISO_ALPHA2', StringType(), False),
 StructField('CAPITAL', StringType(), False),
 StructField('POPULATION', DoubleType(), False),
 StructField('AREA_KM2', IntegerType(), False),
 StructField('REGION_ID', IntegerType(), False),
 StructField('SUB_REGION_ID', IntegerType(), True),
 StructField('INTERMEDIATE_REGION_ID', IntegerType(), True),
 StructField('ORGANIZATION_REGION_ID', IntegerType(), True),
 ]
)

# COMMAND ----------

countries = spark.read.csv(path=countries_path, header=True, schema=counties_schema)


# COMMAND ----------

countries.display()

# COMMAND ----------

countries.select('name', 'capital', 'population').display()

# COMMAND ----------

countries.select(countries['name'], countries['CAPITAL'], countries['PopulatiOn']).display()

# COMMAND ----------

countries.select(countries.NAME, countries.CAPITAL, countries.POPULATION).display()

# COMMAND ----------

from pyspark.sql.functions import col 
countries.select(col('namE'), col('capital'), col('population')).display()

# COMMAND ----------

countries.select(countries['name'].alias('country_name'), countries['capital'].alias('capital_city'), countries['population']).display()

# COMMAND ----------

countries.select('name', 'capital', 'population').withColumnRenamed('name', 'country_name').withColumnRenamed('capital', 'capital_city').display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Adding New Columns

# COMMAND ----------

countries.display()

# COMMAND ----------

from pyspark.sql.functions import current_date 
countries.withColumn('current_date', current_date()).display()

# COMMAND ----------

from pyspark.sql.functions import lit 
countries.withColumn('updated_by', lit('MV')).display()

# COMMAND ----------

countries.withColumn('population_m', countries['population']/1000000).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Changing Data Types

# COMMAND ----------

countries_dt = spark.read.csv(path=countries_path, header=True)

# COMMAND ----------

countries_dt.dtypes

# COMMAND ----------

countries_df2 = countries_df.alias('countries_df2')

# COMMAND ----------

countries_df2.display()

# COMMAND ----------

countries_df2.dtypes

# COMMAND ----------

# Change Datatype
countries_dt.select(countries_dt['population'].cast(IntegerType())).dtypes

# COMMAND ----------

countries_df2.dtypes

# COMMAND ----------

countries.select(countries['population'].cast(StringType())).dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC #Math Functions and Simple Arithmetic

# COMMAND ----------

countries.select(countries['population']/1000000).withColumnRenamed('(population / 1000000)', 'population_m').display()

# COMMAND ----------

countries_2 = countries.select(countries['population']/1000000).withColumnRenamed('(population / 1000000)', 'population_m')

# COMMAND ----------

countries_2.display()

# COMMAND ----------

from pyspark.sql.functions import round
countries_2.select(round(countries_2['population_m'], 3)).display()

# COMMAND ----------

countries.withColumn('population_m', round(countries['population']/1000000, 1)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Sort Functions

# COMMAND ----------

countries.display()

# COMMAND ----------

from pyspark.sql.functions import asc 
countries.sort(countries['population'].asc()).display()

# COMMAND ----------

from pyspark.sql.functions import desc
countries.sort(countries['population'].desc()).display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC # String Function

# COMMAND ----------

countries.select(upper(countries['name'])).display()

# COMMAND ----------

countries.select(initcap(countries['name'])).display()

# COMMAND ----------

countries.select(length(countries['name'])).display()

# COMMAND ----------

countries.display()

# COMMAND ----------

countries.select(concat_ws(' ', countries['name'], countries['country_code'])).display()

# COMMAND ----------

countries.select(concat_ws('-', countries['name'], countries['country_code'])).display()

# COMMAND ----------

countries.select(concat_ws('-', countries['name'], lower(countries['country_code']))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Datetime Functions

# COMMAND ----------

countries = spark.read.csv(path=countries_path, header=True, schema=counties_schema)

# COMMAND ----------

countries.display()

# COMMAND ----------

from pyspark.sql.functions import *
countries = countries.withColumn('timestamp', current_timestamp())

# COMMAND ----------

countries.display()

# COMMAND ----------

countries.select(month('timestamp')).display()

# COMMAND ----------

countries.select(month(countries['timestamp'])).display()


# COMMAND ----------

countries.select(year(countries['timestamp'])).display()

# COMMAND ----------

countries = countries.withColumn('date_literal', lit('27-10-2020'))

# COMMAND ----------

countries.display()

# COMMAND ----------

countries.dtypes

# COMMAND ----------

countries = countries.withColumn('date', to_date(countries['date_literal'], 'dd-MM-yyyy'))

# COMMAND ----------

countries.display()

# COMMAND ----------

countries.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC # Filtering DataFrames

# COMMAND ----------

countries = spark.read.csv(path=countries_path, header=True, schema=counties_schema)

# COMMAND ----------

countries.display()

# COMMAND ----------

len(countries.columns)

# COMMAND ----------

countries.count()

# COMMAND ----------

countries.display()

# COMMAND ----------

countries.filter(countries['population']>1000000).display()

# COMMAND ----------

from pyspark.sql.functions import locate
countries.filter(locate("B", countries['capital'])==1).display()

# COMMAND ----------

countries.filter(  (locate("B", countries['capital'])==1) & (countries['population']>1000000000 ) ).display()

# COMMAND ----------

countries.filter(  (locate("B", countries['capital'])==1) | (countries['population']>1000000000 ) ).display()

# COMMAND ----------

countries.filter("region_id==10").display()

# COMMAND ----------

countries.filter("region_id != 10  and population ==0").display()

# COMMAND ----------

# Assignment
# Filter the records in the countries DataFrame where:
   # The country name is greater than 15 characters and the region id is not 10

countries.filter("len(name) > 15 and region_id !=10").display()

# COMMAND ----------

from pyspark.sql.functions import length
#Solution using SQL commands
countries.filter("length(name) > 15 and region_id !=10").display()

# COMMAND ----------

# Solution without SQL commands
countries.filter( (length(countries["name"]) > 15) & (countries["region_id"] != 10)).display()

# COMMAND ----------

countries.filter( (length(countries["name"]) > 15) & (countries.REGION_ID != 10)).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC # Conditional Statements

# COMMAND ----------

from pyspark.sql.functions import when 
countries.withColumn('name_length', when(countries["population"] > 1000000000, 'large').when(countries["population"] <= 1000000000, 'not large')).display()

# COMMAND ----------

countries.withColumn('name_length', when(countries["population"] > 1000000000, 'large').otherwise('not large')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Expr
# MAGIC expr() let's you execute SQL-like expressions and use a DataFrame column value as an expression argument to Pyspark built-in functions

# COMMAND ----------

countries = spark.read.csv(path=countries_path, header=True, schema=counties_schema)

# COMMAND ----------

from pyspark.sql.functions import expr 

countries.select(expr('NAME as country_name')).display()

# COMMAND ----------

countries.select(expr('left(NAME, 2) as name')).display()

# COMMAND ----------

"""
    If the population is greater than 100 million return 'very large'
    'medium' if it's greater than 50million but less than or equal to 100 million
    'small' otherwise
"""

# COMMAND ----------

countries.withColumn('population_class', expr("case when population > 1000000000 then 'large' when population > 50000000 then 'medium' else \
    'small' end ")).display()

# COMMAND ----------

"""
      Assignment
 Create a new column called area_class

 It should contain the value 'large' for records
 where the area_kms is above 1 million, 'medium' if it's above 300,000 but less than or equal to 1 million otherwise
"""

# COMMAND ----------

# My Solution
countries.withColumn('area_class', expr("case when area_km2 > 1000000 then 'large' when area_km2 > 300000 then 'medium' else 'small' end ")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Removing Columns

# COMMAND ----------

countries = spark.read.csv(path=countries_path, header=True, schema=counties_schema)

# COMMAND ----------

countries_2 = countries.select("name", "capital", 'population')

# COMMAND ----------

countries_2.display()

# COMMAND ----------

countries_3 = countries.drop(countries['organization_region_id'])

# COMMAND ----------

countries_3.display()

# COMMAND ----------

countries_3.drop('sub_region_id', 'intermediate_region_id').display()

# COMMAND ----------

# MAGIC %md 
# MAGIC # Grouping your DataFrame

# COMMAND ----------

countries.groupBy('region_id')

# COMMAND ----------

from pyspark.sql.functions import * 

countries. \
groupBy('region_id'). \
sum('population'). \
display()

# COMMAND ----------

countries. \
groupBy('region_id'). \
min('population'). \
display()

# COMMAND ----------

countries. \
groupBy('region_id'). \
avg('population'). \
display()

# COMMAND ----------

countries. \
groupBy('region_id'). \
sum('population', 'area_km2'). \
display()

# COMMAND ----------

countries.groupBy('region_id').agg(avg('population'), sum('area_km2')).display()

# COMMAND ----------

countries.groupBy('region_id', 'sub_region_id').agg(avg('population'), sum('area_km2')).display()

# COMMAND ----------

# Using withColumnRenamed
countries.groupBy('region_id', 'sub_region_id'). \
agg(sum('population'), sum('area_km2')). \
withColumnRenamed('sum(population)', 'total_pop'). \
withColumnRenamed('sum(area_km2)', 'total_area'). \
display()


# COMMAND ----------

# Using alias
countries.groupBy('region_id', 'sub_region_id'). \
agg(sum('population').alias('total_pop'), sum('area_km2').alias('total_area')). \
display()

# COMMAND ----------

"""
      Assignment
- Aggregate the datafrmae by region_id and sub_region_id

   Display the minimum- and maximum population aliased as max_pop and min_pop

   Sort the results by region_id in ascending order
"""

# COMMAND ----------

# My Solution
countries.groupBy("region_id", 'sub_region_id') \
.agg(min('population').alias('min_pop'), max('population').alias('max_pop')) \
.sort(asc('region_id')) \
.show()

# COMMAND ----------

# The teacher's solution
countries.groupBy("region_id", 'sub_region_id') \
.agg(min('population').alias('min_pop'), max('population').alias('max_pop')) \
.sort(countries['region_id'].asc()) \
.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Pivotting  DataFrame
# MAGIC

# COMMAND ----------

from pyspark.sql.types import IntegerType, DoubleType, StructField, StructType, StringType
# FALSE => It should not be NUll, True=> It can be NULL
# StructType defines the structure of the DataFrame
# StructField defines the metadata of the columns within a DataFrame

counties_schema = StructType(
[
 StructField('COUNTRY_ID', IntegerType(), False),
 StructField('NAME', StringType(), False),
 StructField('NATIONALITY', StringType(), False),
 StructField('COUNTRY_CODE', StringType(), False),
 StructField('ISO_ALPHA2', StringType(), False),
 StructField('CAPITAL', StringType(), False),
 StructField('POPULATION', DoubleType(), False),
 StructField('AREA_KM2', IntegerType(), False),
 StructField('REGION_ID', IntegerType(), False),
 StructField('SUB_REGION_ID', IntegerType(), True),
 StructField('INTERMEDIATE_REGION_ID', IntegerType(), True),
 StructField('ORGANIZATION_REGION_ID', IntegerType(), True),
 ]
)

countries = spark.read.csv(path=countries_path, header=True, schema=counties_schema)

# COMMAND ----------

countries.groupBy('region_id', 'sub_region_id').sum('population').display()

# COMMAND ----------

countries.groupBy('region_id', 'sub_region_id').pivot('region_id').sum('population').display()

# COMMAND ----------

countries.groupBy('sub_region_id').pivot('region_id').sum('population').display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Joining DataFrames

# COMMAND ----------

region_path= "/FileStore/tables/country_regions.csv"

regions = spark.read.csv(region_path, header="True")
display(region_path)

# COMMAND ----------

from pyspark.sql.types import IntegerType, DoubleType, StructField, StructType, StringType
# FALSE => It should not be NUll, True=> It can be NULL
# StructType defines the structure of the DataFrame
# StructField defines the metadata of the columns within a DataFrame

regions_schema = StructType(
[
 StructField('ID', IntegerType(), False),
 StructField('NAME', StringType(), False),
 ]
)

regions = spark.read.csv(path=region_path, header=True, schema=regions_schema)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Inner Join
# MAGIC Inner Join: Returns only matching records from both table
# MAGIC

# COMMAND ----------

countries.display()

# COMMAND ----------

regions.display()

# COMMAND ----------

countries.join(regions, countries['region_id']==regions['Id'], 'inner').display()

# COMMAND ----------

# Assignment
countries \
.join(regions, regions['Id'] == countries['region_id'], 'inner') \
.select(countries['name'].alias('country_name'), regions['name'].alias('region_name'), countries['population']) \
.sort(countries['population'].desc()) \
.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Left Join 
# MAGIC Returns all records from the left table and only matching records from the right table

# COMMAND ----------

countries.join(regions, countries['region_id']==regions['Id'], 'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Right Join
# MAGIC Returns all records from the right table and only matching records from the left table

# COMMAND ----------

countries.join(regions, countries['region_id']==regions['Id'], 'right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC Returns all records from both tables
# MAGIC with NaNs for missing values

# COMMAND ----------

countries.join(regions, countries['region_id']==regions['Id'], 'right') \
.select(countries['name'], regions['name'], countries['population']) \
.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC # Union

# COMMAND ----------

from pyspark.sql.functions import count

countries.count()

# COMMAND ----------

union = countries.union(countries)
union.display()

# COMMAND ----------

union.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Unpivotting DataFrame

# COMMAND ----------

countries \
.join(regions, regions['Id'] == countries['region_id'], 'inner') \
.select(countries['name'].alias('country_name'), regions['name'].alias('region_name'), countries['population']) \
.sort(countries['population'].desc()) \
.display()

# COMMAND ----------

countries = countries \
.join(regions, regions['Id'] == countries['region_id'], 'inner') \
.select(countries['name'].alias('country_name'), regions['name'].alias('region_name'), countries['population'])

# COMMAND ----------

countries.display()

# COMMAND ----------

pivot_countries = countries.groupBy('country_name').pivot('region_name').sum('population')

# COMMAND ----------

pivot_countries.display()

# COMMAND ----------

from pyspark.sql.functions import expr

pivot_countries.select('country_name', expr("stack(5, 'Africa', Africa, 'America', America, 'Asia', Asia, 'Europe', Europe, 'Oceania', Oceania) as (region_name, population)")).filter('population is not null').display()

# COMMAND ----------

# MAGIC %md 
# MAGIC # Pandas

# COMMAND ----------

import pandas as pd 

countries.display()


# COMMAND ----------

countries_pd = countries.toPandas()

# COMMAND ----------

countries_pd

# COMMAND ----------

countries_pd.dtypes

# COMMAND ----------

countries_pd.head()

# COMMAND ----------

countries_pd.iloc[0]

# COMMAND ----------

# MAGIC %md
# MAGIC # Challenge Section: Customer Order

# COMMAND ----------

