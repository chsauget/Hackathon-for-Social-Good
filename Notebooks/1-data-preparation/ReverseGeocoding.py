# Databricks notebook source
spark.conf.set("spark.databricks.io.cache.enabled", "true")

# COMMAND ----------

import reverse_geocoder as rg
import numpy
import pandas as pd
from pyspark.sql import functions as f

folderPath = '/dbfs/mnt/datasets/geography-country-details/parquet'
dbutils.fs.mkdirs('file:{0}'.format(folderPath))


coordinates = []
for lat in numpy.arange(-90, 90.1, 0.1):
  for lg in numpy.arange(-180, 180.1, 0.1):
    coordinates.append((lat,lg))

print(len(coordinates))
results = rg.search(coordinates)

df = pd.DataFrame(results)
dfCoordinated = pd.DataFrame(coordinates)
df = pd.concat([df, dfCoordinated], axis=1)
ReversedDf = df.rename(columns={0: "InitialLat", 1: "InitialLong","lat":"NearestLat","lon":"NearestLon","name":"City","cc":"countryCode2"})
countryDf = spark.createDataFrame(ReversedDf)

databaseLocation = '/mnt/datawarehouse'
databaseName = 'SelfServiceWareHouse'
tableName = 'dimGeo'

countryDf = countryDf.withColumn("geoKey",f.concat(f.format_number(countryDf.InitialLat,1),f.lit('|'),f.format_number(countryDf.InitialLong,1)))
countryDf.write.option("mergeSchema", "true").mode("overwrite").partitionBy('countryCode2').format("delta").save("%s/%s"%(databaseLocation,tableName))

spark.sql("DROP TABLE IF EXISTS %s.%s"%(databaseName,tableName))
spark.sql("CREATE TABLE %s.%s USING DELTA LOCATION '%s/%s'"%(databaseName,tableName,databaseLocation,tableName))


# COMMAND ----------

import numpy
coordinates = []
for lat in numpy.arange(-90, 90.01, 0.01):
  for lg in numpy.arange(-180, 180.01, 0.01):
    coordinates.append((lat,lg))

print(len(coordinates))

# COMMAND ----------

spark.sql("OPTIMIZE SelfServiceWareHouse.dimGeo ZORDER BY (geoKey)")