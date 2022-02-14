# Databricks notebook source
from pyspark.sql.types import *

path = "/FileStore/tables/salesLog"

# create schema for data so stream processing is faster
salesSchema = StructType([
  StructField("OrderID", DoubleType(), True),
  StructField("OrderDate", StringType(), True),
  StructField("Quantity", DoubleType(), True),
  StructField("DiscountPct", DoubleType(), True),
  StructField("Rate", DoubleType(), True),
  StructField("SaleAmount", DoubleType(), True),
  StructField("CustomerName", StringType(), True),
  StructField("State", StringType(), True),
  StructField("Region", StringType(), True),
  StructField("ProductKey", StringType(), True),
  StructField("RowCount", DoubleType(), True),
  StructField("ProfitMargin", DoubleType(), True)])

# Static DataFrame containing all the files in sales_log
data = (
  spark
    .read
    .schema(salesSchema)
    .csv(path)
)

display(data)

# COMMAND ----------

#spark.sql("DROP TABLE "+ table_name)


# Define the input and output formats and paths and the table name.
#read_format = 'delta'
write_format = 'delta'
save_path = '/FileStore/tables/salesLogX'
table_name = 'salesLogX'
partition_by='State'

# Delete the saved data.
dbutils.fs.rm(save_path, True)

# Write the data to its target.
data.write \
  .format(write_format) \
  .partitionBy(partition_by)\
  .mode("overwrite") \
  .save(save_path)

# Create the table.
spark.sql("CREATE TABLE " + table_name + " USING DELTA LOCATION '" + save_path + "'")

display(spark.sql("SELECT * FROM salesLogX"))

# COMMAND ----------

display(spark.sql("SELECT * FROM salesLogX where State='Florida' and CustomerName='Tanja Collins'"))
#1.64  without partition

# 1.37 with partition

# COMMAND ----------

display(spark.sql("SELECT -99,CustomerName FROM salesLogX where State='Florida' and CustomerName='Tanja Collins' and Quantity=30"))

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO  salesLogX PARTITION (State = 'Florida')
# MAGIC     SELECT -99,OrderDate,
# MAGIC Quantity,
# MAGIC DiscountPct,
# MAGIC Rate,
# MAGIC SaleAmount,
# MAGIC CustomerName,
# MAGIC Region,
# MAGIC ProductKey,
# MAGIC RowCount,
# MAGIC ProfitMargin FROM salesLogX where State='Florida' and CustomerName='Tanja Collins' and Quantity=30

# COMMAND ----------

# read delta log file

path2="/FileStore/tables/salesLogX/_delta_log/00000000000000000001.json"

data2 = spark.read.csv(path2, header="true", inferSchema="true")

display(data2)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CREATE TABLE  salesLogX;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create delta table with SQL
# MAGIC -- The path in LOCATION must already exist
# MAGIC -- and must be in Delta format.
# MAGIC 
# MAGIC CREATE TABLE sales_deneme (
# MAGIC `OrderID` DOUBLE, `OrderDate` STRING, `Quantity` DOUBLE, `DiscountPct` DOUBLE, `Rate` DOUBLE, `SaleAmount` DOUBLE, `CustomerName` STRING, `State` STRING, `Region` STRING, `ProductKey` STRING, `RowCount` DOUBLE, `ProfitMargin` DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (state)
# MAGIC LOCATION '/FileStore/tables/salesLogX'

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from sales_deneme



