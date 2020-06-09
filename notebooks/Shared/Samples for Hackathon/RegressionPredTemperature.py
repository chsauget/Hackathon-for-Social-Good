# Databricks notebook source
# DBTITLE 1,Import Data

df_consolidated = spark.sql("SELECT \
  YEAR(date)*10000+MONTH(date)*100+DAY(date) as date, geoKey, \
  avg(surface) as surfaceTemp, avg(t2m) as t2mTemp ,\
  avg(latitude) as latitude, \
  avg(longitude) as longitude \
  FROM selfservicewarehouse.ecv_for_climate_change_surface_air_temperature \
  group by date, geoKey ")


# COMMAND ----------

#dsTest=df_consolidated.limit(1)
#display(dsTest)
# ajout de 6 ans à chaque date 
dsTest= spark.sql("SELECT \
 (YEAR(date)*10000+MONTH(date)*100+DAY(date))+60000 as date , geoKey, \
  avg(surface) as surfaceTemp, avg(t2m) as t2mTemp ,\
  avg(latitude) as latitude, \
  avg(longitude) as longitude \
  FROM selfservicewarehouse.ecv_for_climate_change_surface_air_temperature \
  group by date, geoKey ")


# COMMAND ----------

display(dsTest)

# COMMAND ----------

#display(df_consolidated)

# COMMAND ----------

#df=df_consolidated.where(df_consolidated.co2Diox>0)
#display(df)

# COMMAND ----------

# DBTITLE 1,Prepare Data
from pyspark.sql.functions import col
# drop rows with missing values
data = df_consolidated.dropna() 
exprs = [col(column).alias(column.replace(' ', '_')) for column in data.columns]
data

# COMMAND ----------

# DBTITLE 1,Show Data Schema
data.printSchema()

# COMMAND ----------

from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
#DROP DATE 
featureassembler=VectorAssembler(inputCols=[ 'date','longitude','latitude'], outputCol= 'Features')
#create features for the test pool
outputTest=featureassembler.transform(dsTest)


# COMMAND ----------


display(outputTest)

# COMMAND ----------

output=featureassembler.transform(data)
display(output)
#output.show()

# COMMAND ----------

output.columns

# COMMAND ----------

finalized_dataTest=outputTest.select('Features','t2mTemp')
#Display features and co2 for the test pool
finalized_dataTest.show()

# COMMAND ----------

finalized_data=output.select('Features','t2mTemp')
finalized_data.show()

# COMMAND ----------

# DBTITLE 1,Split data
train_data,test_data=finalized_data.randomSplit([0.8,0.2])

# COMMAND ----------

# DBTITLE 1,Train data with LR
from pyspark.ml.regression import LinearRegression
regressor= LinearRegression(featuresCol='Features',labelCol='t2mTemp')
regressor=regressor.fit(train_data)

# COMMAND ----------

# DBTITLE 1,Regression Coefficients
regressor.coefficients

# COMMAND ----------

regressor.intercept

# COMMAND ----------

# DBTITLE 1,Evaluate model with test data
pred_results=regressor.evaluate(test_data)
pred_resultsTest=regressor.evaluate(finalized_dataTest)

# COMMAND ----------

pred_results.predictions.show()

# COMMAND ----------

# DBTITLE 1,Predicted temperature

display(pred_resultsTest.predictions.select('Features','t2mTemp'))

# COMMAND ----------



# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt

x = output.rdd.map(lambda p: p.date).collect()
y = output.rdd.map(lambda p: (p.t2mTemp)).collect()
#print (x)

plt.style.use('classic')
plt.rcParams['lines.linewidth'] = 0
fig, ax = plt.subplots()
ax.loglog(x,y)
#plt.xlim(1.0e5, 1.0e7)
#plt.ylim(5.0e1, 1.0e3)
ax.scatter(x, y, c="blue")

display(fig)



# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt

x = outputTest.rdd.map(lambda p: p.Features[0]).collect()
y = outputTest.rdd.map(lambda p: (p.t2mTemp)).collect()

plt.style.use('classic')              
plt.rcParams['lines.linewidth'] = 0
fig, ax = plt.subplots()
ax.loglog(x,y)
ax.scatter(x, y, c="blue")
display(fig)

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt

x = output.rdd.map(lambda p: p.Features[0]).collect()
y = output.rdd.map(lambda p: (p.t2mTemp)).collect()
#print (x)

plt.style.use('classic')              
plt.rcParams['lines.linewidth'] = 0
fig, ax = plt.subplots()
ax.loglog(x,y)
#plt.xlim(1.0e5, 1.0e7)
#plt.ylim(5.0e1, 1.0e3)
ax.scatter(x, y, c="blue")

display(fig)

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt

x = output.rdd.map(lambda p: p.Features[0]).collect()
y = output.rdd.map(lambda p: (p.t2mTemp)).collect()
#print (x)

plt.style.use('fivethirtyeight')              
plt.rcParams['lines.linewidth'] = 0
fig, ax = plt.subplots()
ax.loglog(x,y)
#plt.xlim(1.0e5, 1.0e7)
#plt.ylim(1.0e2.5, 1.0e3)
ax.autoscale_view()
ax.scatter(x, y, c="blue")

display(fig)