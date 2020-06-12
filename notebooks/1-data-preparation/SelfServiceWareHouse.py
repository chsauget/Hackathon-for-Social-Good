# Databricks notebook source
# MAGIC %md
# MAGIC #SelfServiceWareHouse

# COMMAND ----------

from pyspark.sql import functions as f
databaseLocation = '/mnt/datawarehouse'
databaseName = 'SelfServiceWareHouse'
spark.sql("CREATE DATABASE IF NOT EXISTS %s LOCATION '%s'"%(databaseName,databaseLocation)) 

# COMMAND ----------

spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")

# COMMAND ----------

dfGeo = spark.sql("SELECT geoKey,city,countryCode2,admin1,admin2 FROM SelfServiceWareHouse.dimGeo").alias('g')

# COMMAND ----------

df = spark.sql("SELECT DISTINCT city FROM SelfServiceWareHouse.dimGeo")
tableName = 'dimCity'
df.write.option("mergeSchema", "true").mode("overwrite").format("delta").save("%s/%s"%(databaseLocation,tableName))
spark.sql("DROP TABLE IF EXISTS %s.%s"%(databaseName,tableName))
spark.sql("CREATE TABLE %s.%s USING DELTA LOCATION '%s/%s'"%(databaseName,tableName,databaseLocation,tableName))

df = spark.sql("SELECT DISTINCT countryCode2 FROM SelfServiceWareHouse.dimGeo")
tableName = 'dimCountry'
df.write.option("mergeSchema", "true").mode("overwrite").format("delta").save("%s/%s"%(databaseLocation,tableName))
spark.sql("DROP TABLE IF EXISTS %s.%s"%(databaseName,tableName))
spark.sql("CREATE TABLE %s.%s USING DELTA LOCATION '%s/%s'"%(databaseName,tableName,databaseLocation,tableName))

df = spark.sql("SELECT DISTINCT geoKey,round(InitialLat,1) AS RoundedLat,round(InitialLong,1) AS RoundedLong FROM SelfServiceWareHouse.dimGeo")
tableName = 'dimGeoRounded'
df.write.option("mergeSchema", "true").mode("overwrite").format("delta").save("%s/%s"%(databaseLocation,tableName))
spark.sql("DROP TABLE IF EXISTS %s.%s"%(databaseName,tableName))
spark.sql("CREATE TABLE %s.%s USING DELTA LOCATION '%s/%s'"%(databaseName,tableName,databaseLocation,tableName))

# COMMAND ----------

tableName = 'satellite_carbon_dioxide'

df = sqlContext.read.parquet("/mnt/datasets/satellite-carbon-dioxide/parquet/")
df = df.withColumn("geoKey",f.concat(f.format_number(df.latitude, 1),f.lit('|'),f.format_number(df.longitude, 1)))\
        .withColumn("date",f.to_date(df.datetime))
df = df.alias('d').join(dfGeo,df.geoKey==dfGeo.geoKey).select('d.*','g.city','g.countryCode2','g.admin1','g.admin2')
df.write.option("mergeSchema", "true").mode("overwrite").partitionBy('countryCode2').format("delta").save("%s/%s"%(databaseLocation,tableName))
spark.sql("DROP TABLE IF EXISTS %s.%s"%(databaseName,tableName))
spark.sql("CREATE TABLE %s.%s USING DELTA LOCATION '%s/%s'"%(databaseName,tableName,databaseLocation,tableName))

# COMMAND ----------

spark.sql("OPTIMIZE SelfServiceWareHouse.satellite_carbon_dioxide ZORDER BY (admin2,admin1,city,date,xco2)")

# COMMAND ----------

tableName = 'ecv-for-climate-change-0_7cm_volumetric_soil_moisture'

df = sqlContext.read.parquet("/mnt/datasets/%s/parquet/"%(tableName))
df = df.withColumn("geoKey",f.concat(f.format_number(df.latitude, 1),f.lit('|'),f.format_number(df.longitude, 1)))\
        .withColumn("date",f.to_date(df.datetime))

df = df.alias('d').join(dfGeo,df.geoKey==dfGeo.geoKey).select('d.*','g.city','g.countryCode2','g.admin1','g.admin2')

df.write.option("mergeSchema", "true").mode("overwrite").partitionBy('countryCode2').format("delta").save("%s/%s"%(databaseLocation,tableName.replace('-','_')))

spark.sql("DROP TABLE IF EXISTS %s.%s"%(databaseName,tableName.replace('-','_')))
spark.sql("CREATE TABLE %s.%s USING DELTA LOCATION '%s/%s'"%(databaseName,tableName.replace('-','_'),databaseLocation,tableName.replace('-','_')))

# COMMAND ----------

spark.sql("OPTIMIZE SelfServiceWareHouse.ecv_for_climate_change_0_7cm_volumetric_soil_moisture ZORDER BY (admin2,admin1,city,date,swvl1)")

# COMMAND ----------

tableName = 'ecv-for-climate-change-precipitation'

df = sqlContext.read.parquet("/mnt/datasets/%s/parquet/"%(tableName))
df = df.withColumn("geoKey",f.concat(f.format_number(df.latitude, 1),f.lit('|'),f.format_number(df.longitude, 1)))\
        .withColumn("date",f.to_date(df.datetime))

df = df.alias('d').join(dfGeo,df.geoKey==dfGeo.geoKey).select('d.*','g.city','g.countryCode2','g.admin1','g.admin2')

df.write.option("mergeSchema", "true").mode("overwrite").partitionBy('countryCode2').format("delta").save("%s/%s"%(databaseLocation,tableName.replace('-','_')))

spark.sql("DROP TABLE IF EXISTS %s.%s"%(databaseName,tableName.replace('-','_')))
spark.sql("CREATE TABLE %s.%s USING DELTA LOCATION '%s/%s'"%(databaseName,tableName.replace('-','_'),databaseLocation,tableName.replace('-','_')))



# COMMAND ----------

spark.sql("OPTIMIZE SelfServiceWareHouse.ecv_for_climate_change_precipitation ZORDER BY (admin2,admin1,city,date,tp)")

# COMMAND ----------

tableName = 'ecv-for-climate-change-sea_ice_cover'

df = sqlContext.read.parquet("/mnt/datasets/%s/parquet/"%(tableName))
df = df.withColumn("geoKey",f.concat(f.format_number(df.latitude, 1),f.lit('|'),f.format_number(df.longitude, 1)))\
        .withColumn("date",f.to_date(df.datetime))

df = df.alias('d').join(dfGeo,df.geoKey==dfGeo.geoKey).select('d.*','g.city','g.countryCode2','g.admin1','g.admin2')

df.write.option("mergeSchema", "true").mode("overwrite").partitionBy('countryCode2').format("delta").save("%s/%s"%(databaseLocation,tableName.replace('-','_')))

spark.sql("DROP TABLE IF EXISTS %s.%s"%(databaseName,tableName.replace('-','_')))
spark.sql("CREATE TABLE %s.%s USING DELTA LOCATION '%s/%s'"%(databaseName,tableName.replace('-','_'),databaseLocation,tableName.replace('-','_')))

# COMMAND ----------

tableName = 'ecv-for-climate-change-sea_ice_cover'
spark.sql("OPTIMIZE SelfServiceWareHouse.%s ZORDER BY (admin2,admin1,city,date,siconc)"%tableName.replace('-','_'))

# COMMAND ----------

tableName = 'ecv-for-climate-change-surface_air_relative_humidity'

df = sqlContext.read.parquet("/mnt/datasets/%s/parquet/"%(tableName))
df = df.withColumn("geoKey",f.concat(f.format_number(df.latitude, 1),f.lit('|'),f.format_number(df.longitude, 1)))\
        .withColumn("date",f.to_date(df.datetime))

df = df.alias('d').join(dfGeo,df.geoKey==dfGeo.geoKey).select('d.*','g.city','g.countryCode2','g.admin1','g.admin2')

df.write.option("mergeSchema", "true").mode("overwrite").partitionBy('countryCode2').format("delta").save("%s/%s"%(databaseLocation,tableName.replace('-','_')))

spark.sql("DROP TABLE IF EXISTS %s.%s"%(databaseName,tableName.replace('-','_')))
spark.sql("CREATE TABLE %s.%s USING DELTA LOCATION '%s/%s'"%(databaseName,tableName.replace('-','_'),databaseLocation,tableName.replace('-','_')))

# COMMAND ----------

tableName = 'ecv-for-climate-change-surface_air_relative_humidity'
spark.sql("OPTIMIZE SelfServiceWareHouse.%s ZORDER BY (admin2,admin1,city,date,r)"%tableName.replace('-','_'))

# COMMAND ----------

tableName = 'ecv-for-climate-change-surface_air_temperature'

df = sqlContext.read.parquet("/mnt/datasets/%s/parquet/"%(tableName))
df = df.withColumn("geoKey",f.concat(f.format_number(df.latitude, 1),f.lit('|'),f.format_number(df.longitude, 1)))\
        .withColumn("date",f.to_date(df.datetime))

df = df.alias('d').join(dfGeo,df.geoKey==dfGeo.geoKey).select('d.*','g.city','g.countryCode2','g.admin1','g.admin2')

df.write.option("mergeSchema", "true").mode("overwrite").partitionBy('countryCode2').format("delta").save("%s/%s"%(databaseLocation,tableName.replace('-','_')))

spark.sql("DROP TABLE IF EXISTS %s.%s"%(databaseName,tableName.replace('-','_')))
spark.sql("CREATE TABLE %s.%s USING DELTA LOCATION '%s/%s'"%(databaseName,tableName.replace('-','_'),databaseLocation,tableName.replace('-','_')))

# COMMAND ----------

tableName = 'ecv-for-climate-change-surface_air_temperature'
spark.sql("OPTIMIZE SelfServiceWareHouse.%s ZORDER BY (admin2,admin1,city,date,t2m)"%tableName.replace('-','_'))

# COMMAND ----------

import pandas as pd
from pyspark.sql import Row

l = pd.date_range(start="1980-01-01",end="2019-12-31").tolist()

df = spark.createDataFrame(list(map(lambda x: Row(date=x.date()), l)))
df = df.withColumn("year",f.year(df.date))\
        .withColumn("monthNumber",f.month(df.date)) \
        .withColumn("month",f.date_format(df.date,"MMMM")) \
        .withColumn("monthYear",f.concat(f.date_format(df.date,"MMMM"),f.lit(', '), f.year(df.date)))

df.write.option("mergeSchema", "true").mode("overwrite").format("delta").save("%s/%s"%(databaseLocation,'DimDate'))
spark.sql("DROP TABLE IF EXISTS %s.%s"%(databaseName,'DimDate'))
spark.sql("CREATE TABLE %s.%s USING DELTA LOCATION '%s/%s'"%(databaseName,'DimDate',databaseLocation,'DimDate'))