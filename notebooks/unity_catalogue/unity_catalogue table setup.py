# Databricks notebook source
# MAGIC %md
# MAGIC # Setting up a table in unity catalogue

# COMMAND ----------

#use dabds_qa catalogue
spark.sql("USE CATALOG dabds_qa")

# COMMAND ----------

# create a dff database in catalogue
project_name = 'dff'
query  = f"""CREATE SCHEMA IF NOT EXISTS dabds_qa.{project_name}"""
spark.sql(query)

# COMMAND ----------

# create a table 'test' in that database
# You would do something like this to create bronze, silver, gold tables
# table_name = 'test'
# query  = f"""CREATE TABLE IF NOT EXISTS dabds_qa.{project_name}.{table_name}
# """
# spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Versioning in databricks tables

# COMMAND ----------

# MAGIC %md
# MAGIC Loading some test data to save to this table

# COMMAND ----------

import pyspark.pandas as ps
psdf = ps.read_table('hive_metastore.dabo_datamarts_hvc.fact_fragmentation')

# COMMAND ----------

# let's use the top 10 rows to make everything faster
psdf_small = psdf.head(10)
psdf_small

# COMMAND ----------

#remove any prior saves
ps.sql("DROP TABLE if EXISTS dabds_qa.dff.test_small")


# COMMAND ----------

table_mode = 'overwrite' # or 'append' or other options in docs
psdf_small.to_table('dabds_qa.dff.test_small',
                    mode = table_mode)


# COMMAND ----------

# MAGIC %md
# MAGIC pand.to_table() options docs https://spark.apache.org/docs/3.2.1/api/python/reference/pyspark.pandas/api/pyspark.pandas.DataFrame.to_table.html

# COMMAND ----------

# MAGIC %md
# MAGIC Built-in schema validation in databricks. We'll create two new columns, and databricks will throw an error, since this schema won't match the existing schema

# COMMAND ----------

psdf_small.loc[:, 'LONGITUDE_DIFF'] = psdf_small['LONGITUDE'] - psdf_small['FRAGTRACK_LONGITUDE'] 
psdf_small.loc[:, 'LATITUDE_DIFF'] = psdf_small['LATITUDE'] - psdf_small['FRAGTRACK_LATITUDE'] 

# COMMAND ----------

# psdf_small.to_table('dabds_qa.dff.test_small',
#                     mode = table_mode)

## shoudl produce an error:AnalysisException: A schema mismatch detected when writing to the Delta table (Table ID: 45271433-38e4-4ad9-86aa-c8a533dab949).

# COMMAND ----------

# MAGIC %md
# MAGIC Drop the two new columns, so that the schema

# COMMAND ----------

psdf_small = psdf_small.drop(columns = ['LONGITUDE_DIFF', 'LATITUDE_DIFF'])

# COMMAND ----------

# MAGIC %md
# MAGIC Edit data to save a new version of the table. 

# COMMAND ----------

psdf_small.loc[:,'FRAGTRACK_LONGITUDE'] = psdf_small['LONGITUDE']

# COMMAND ----------

psdf_small.to_table('dabds_qa.dff.test_small',
                    mode = table_mode)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Now let's read in two different versions and compare them

# COMMAND ----------

# MAGIC %md
# MAGIC docs https://docs.databricks.com/en/delta/history.html

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY dabds_qa.dff.test_small

# COMMAND ----------

# MAGIC %md
# MAGIC Use VERSION argument to fetch correct dataset version. Version 0 should have a mismatch in longitude, version 1 should have same longitudes

# COMMAND ----------

psdf_earliest = ps.sql('SELECT * FROM dabds_qa.dff.test_small VERSION AS OF 0').head(1)
psdf_earliest[['LONGITUDE', 'FRAGTRACK_LONGITUDE']]

# COMMAND ----------

psdf_latest = ps.sql('SELECT * FROM dabds_qa.dff.test_small VERSION AS OF 1').head(1)
psdf_latest[['LONGITUDE', 'FRAGTRACK_LONGITUDE']]

# COMMAND ----------

# MAGIC %md
# MAGIC Use timestamp argument to fetch correct dataset version

# COMMAND ----------

## WIll need the correct timestamp there
# psdf_earliest = ps.sql("SELECT * FROM dabds_qa.dff.test_small TIMESTAMP AS OF '2023-09-15T22:26:41.000+0000'").head(1)
# psdf_earliest[['LONGITUDE', 'FRAGTRACK_LONGITUDE']]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Z-ordering and optimization

# COMMAND ----------

# MAGIC %md
# MAGIC First, write full frag_track table to unity catalogue. Drop previous tables for consistency. Optimization would not be noticable on atable with only 10 rows, so we'd need a larger one

# COMMAND ----------

import pyspark.pandas as ps
display(ps.sql("DROP TABLE if EXISTS dabds_qa.dff.test"))
psdf = ps.read_table('hive_metastore.dabo_datamarts_hvc.fact_fragmentation')
psdf.to_table('dabds_qa.dff.test',
                )

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY dabds_qa.dff.test

# COMMAND ----------

import time
start = time.time()

df_test = ps.sql("SELECT PATTERN_ID, WENCO_LOAD_BENCH_ELEVATION, AVG(new_bin0) AS new_bin0_avg FROM dabds_qa.dff.test GROUP BY PATTERN_ID, WENCO_LOAD_BENCH_ELEVATION"
)

end = time.time()
diff=end - start
print(diff)

# COMMAND ----------

df_test.head()

# COMMAND ----------

# MAGIC %md
# MAGIC z-order by elevation, since we excpect a loop to read in elevations one-by-one. This would only make sense if we read one-by-one from storage. If we load the full frame and then loop over it, there won't be an increase in performance

# COMMAND ----------

# optimize - will compact the table. It'll get rid of small delta files and keep only the larger ones
# zorder - it will make sure similar column values are stored close by

display(ps.sql("DROP TABLE if EXISTS dabds_qa.dff.test"))
psdf.to_table('dabds_qa.dff.test')
display(ps.sql('OPTIMIZE dabds_qa.dff.test ZORDER BY (WENCO_LOAD_BENCH_ELEVATION)'))


# COMMAND ----------

# MAGIC %md
# MAGIC optimze docs https://docs.databricks.com/en/sql/language-manual/delta-optimize.html

# COMMAND ----------

start = time.time()

df_test = ps.sql("SELECT PATTERN_ID, WENCO_LOAD_BENCH_ELEVATION, AVG(new_bin0) AS new_bin0_avg FROM dabds_qa.dff.test GROUP BY PATTERN_ID, WENCO_LOAD_BENCH_ELEVATION"
)

end = time.time()
diff=end - start
print(diff)

# COMMAND ----------


